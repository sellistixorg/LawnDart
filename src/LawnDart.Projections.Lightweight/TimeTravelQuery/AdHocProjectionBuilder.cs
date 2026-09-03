using System.Text.Json;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Sdk;

namespace LawnDart.Projections.Lightweight.TimeTravelQuery;

/// <summary>
/// Performs an ephemeral, ad-hoc projection replay up to a specified point in time.
/// No checkpoints or view-store writes are performed — the result is purely in-memory.
/// </summary>
/// <remarks>
/// <para>
/// Read strategy is optimized per projection kind to minimize the number of events
/// scanned:
/// <list type="bullet">
///   <item><b>SingleStream + AtVersion/AtTimestamp</b> — uses <c>ReadStreamEnumerableAsync</c> with native bounds.</item>
///   <item><b>SingleStream + AtSequence</b> — reads the stream and stops when the sequence boundary is crossed.</item>
///   <item><b>DCB / MultiStream (typed)</b> — uses <c>ReadByQueryStreamAsync</c> with the event-type filter plus the requested upper bound.</item>
///   <item><b>Global / MultiStream (untyped)</b> — uses <c>ReadByQueryStreamAsync(Query.All())</c> with the upper bound.</item>
/// </list>
/// </para>
/// <para>
/// Per-instance filtering (stream-id match, MultiStream entity-resolver match) is
/// handled by the shared <see cref="ProjectionEventFilter"/> helper, which is also
/// used by <see cref="LawnDart.Projections.Lightweight.Debug.ProjectionTimelineService"/>.
/// </para>
/// </remarks>
public sealed class AdHocProjectionBuilder(
    IEventStore eventStore,
    IReadOnlyList<ProjectionRegistration> registrations)
{
    private readonly ProjectionRegistrationCatalog _catalog = new(registrations);

    /// <summary>
    /// Replays the named projection for the given instance up to the specified cutoff and returns the view state.
    /// </summary>
    /// <param name="projectionName">The registered projection name, e.g. "OrderSummary".</param>
    /// <param name="instanceId">
    /// The view instance key.  For <c>SingleStream</c> this is the full stream ID (e.g. <c>buyers:OrderAggregate:guid</c>).
    /// For <c>MultiStream</c> this is the entity key (e.g. <c>tenant:guid</c> or just <c>guid</c>).
    /// For <c>Global</c> / <c>Dcb</c> this is <c>"global"</c> (single-node) or
    /// <c>global:n{NodeInstance}</c> when <c>TotalInstances &gt; 1</c>.
    /// </param>
    /// <param name="cutoff">The point-in-time boundary — see <see cref="ProjectionCutoff"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="version">
    /// Optional projection implementation version. When omitted, the latest/default version is used.
    /// </param>
    /// <returns>A <see cref="TimeTravelResult"/> with the view state and replay metadata.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the projection name is not found, or when <see cref="CutoffKind.Version"/> is used
    /// with a non-SingleStream projection.
    /// </exception>
    public async Task<TimeTravelResult> BuildAtAsync(
        string projectionName,
        string instanceId,
        ProjectionCutoff cutoff,
        CancellationToken cancellationToken = default,
        int? version = null)
    {
        var (registration, handler, processEvent, getView, entityResolver) =
            CreateReplayContext(projectionName, version);

        if (cutoff.Kind == CutoffKind.Version && registration.Kind != ProjectionKind.SingleStream)
            throw new InvalidOperationException(
                $"Cutoff kind '{nameof(CutoffKind.Version)}' is only valid for SingleStream projections. " +
                $"Projection '{projectionName}' is kind '{registration.Kind}'. " +
                $"Use AtSequence or AtTimestamp instead.");

        long eventsApplied       = 0;
        long? finalSequencePos   = null;
        long? finalStreamVersion = null;
        DateTime? finalTimestamp = null;

        await foreach (var se in ProjectionEventFilter
            .GetAppliedEventsAsync(eventStore, registration, instanceId, cutoff, entityResolver, cancellationToken)
            .ConfigureAwait(false))
        {
            processEvent(handler, se.Event, se.SequencePosition);
            eventsApplied++;
            finalSequencePos   = se.SequencePosition;
            finalStreamVersion = se.Version;
            finalTimestamp     = se.Metadata.Timestamp;
        }

        var viewObj  = getView(handler);
        var viewJson = JsonSerializer.Serialize(viewObj, viewObj.GetType(), ProjectionViewJson.Write);

        return new TimeTravelResult
        {
            LogicalName           = registration.LogicalName,
            ProjectionVersion     = registration.Version,
            ProjectionStorageKey  = registration.StorageKey,
            IsLatest              = registration.IsLatest,
            LatestResolutionMode  = registration.LatestResolutionMode.ToString(),
            InstanceId            = instanceId,
            ViewJson              = viewJson,
            EventsApplied         = eventsApplied,
            FinalSequencePosition = finalSequencePos,
            FinalStreamVersion    = finalStreamVersion,
            FinalEventTimestamp   = finalTimestamp,
            RequestedCutoff       = cutoff.Description,
            ProjectionKind        = registration.Kind.ToString()
        };
    }

    // ── Shared replay context factory ─────────────────────────────────────────

    internal (
        ProjectionRegistration Registration,
        object Handler,
        Action<object, IEvent, long> ProcessEvent,
        Func<object, object> GetView,
        IMultiStreamEntityResolver? EntityResolver)
    CreateReplayContext(string projectionName, int? version = null)
    {
        var registration = _catalog.Resolve(projectionName, version);

        var handler = Activator.CreateInstance(registration.HandlerType)
            ?? throw new InvalidOperationException(
                $"Cannot create an instance of handler type '{registration.HandlerType.FullName}'.");

        var descriptor = new ProjectionDescriptor
        {
            ProjectionType = registration.StorageKey,
            HandlerType    = registration.HandlerType,
            ViewType       = registration.ViewType,
            Version        = registration.Version,
            StreamTypes    = registration.StreamType is not null ? [registration.StreamType] : []
        };

        var processEvent = descriptor.GetCompiledProcessEvent();
        var getView      = descriptor.GetCompiledGetView();

        IMultiStreamEntityResolver? entityResolver = null;
        if (registration.Kind == ProjectionKind.MultiStream)
        {
            entityResolver = handler as IMultiStreamEntityResolver
                ?? throw new InvalidOperationException(
                    $"Projection '{registration.DisplayName}' (MultiStream) handler does not implement IMultiStreamEntityResolver.");
        }

        return (registration, handler, processEvent, getView, entityResolver);
    }
}
