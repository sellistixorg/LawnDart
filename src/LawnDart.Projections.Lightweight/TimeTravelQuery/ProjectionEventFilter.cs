using System.Runtime.CompilerServices;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight.Registration;

namespace LawnDart.Projections.Lightweight.TimeTravelQuery;

/// <summary>
/// Internal helper that produces the stream of events a specific projection instance
/// would apply, using the same read strategy and per-instance filtering rules as the
/// live <c>LightweightProjectionRunnerService</c>.
/// </summary>
/// <remarks>
/// Shared by <see cref="AdHocProjectionBuilder"/> and
/// <see cref="LawnDart.Projections.Lightweight.Debug.ProjectionTimelineService"/> so the filtering contract
/// is defined in exactly one place.
/// </remarks>
internal static class ProjectionEventFilter
{
    /// <summary>
    /// Returns an async sequence of <see cref="SequencedEvent"/> values that the named
    /// projection instance would apply, respecting the optional upper-bound cutoff and
    /// all per-kind filtering rules (stream-id match for SingleStream, entity-resolver
    /// match for MultiStream, DCB type filter for Dcb/MultiStream).
    /// </summary>
    /// <param name="eventStore">The event store to read from.</param>
    /// <param name="registration">The projection registration describing kind, stream type, and DCB types.</param>
    /// <param name="instanceId">
    /// The view instance key — stream ID for SingleStream, entity key for MultiStream,
    /// or <c>"global"</c> for Global / Dcb.
    /// </param>
    /// <param name="cutoff">
    /// Optional upper-bound cutoff; <see langword="null"/> means read all events with no upper bound.
    /// </param>
    /// <param name="resolver">
    /// MultiStream entity resolver; required when <paramref name="registration"/> kind is
    /// <see cref="ProjectionKind.MultiStream"/>, ignored otherwise.
    /// </param>
    /// <param name="ct">Cancellation token checked before each yielded event.</param>
    internal static async IAsyncEnumerable<SequencedEvent> GetAppliedEventsAsync(
        IEventStore eventStore,
        ProjectionRegistration registration,
        string instanceId,
        ProjectionCutoff? cutoff,
        IMultiStreamEntityResolver? resolver,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var rawStream = BuildRawStream(eventStore, registration, instanceId, cutoff, ct);

        await foreach (var se in rawStream.WithCancellation(ct).ConfigureAwait(false))
        {
            // SingleStream + AtSequence: the stream is read open-ended; stop here.
            if (cutoff?.Kind == CutoffKind.Sequence
                && se.SequencePosition > cutoff.SequencePosition!.Value)
                yield break;

            // SingleStream: only yield events belonging to the requested instance.
            // TenantScoped: instanceId is the full stream ID → exact match.
            // SystemGlobal/TenantGlobal: the runner strips the tenant prefix before saving,
            //   so instanceId may be just the bare aggregate ID → match via StreamIdParser.
            //   If the caller did supply a full stream ID (contains ':'), fall back to exact match.
            if (registration.Kind == ProjectionKind.SingleStream)
            {
                bool matches = IsFullStreamId(registration, instanceId)
                    ? string.Equals(se.StreamId, instanceId, StringComparison.OrdinalIgnoreCase)
                    : string.Equals(
                        StreamIdParser.ExtractAggregateIdString(se.StreamId),
                        instanceId,
                        StringComparison.OrdinalIgnoreCase);

                if (!matches)
                    continue;
            }

            // MultiStream: route via entity resolver — skip events for other entities.
            if (registration.Kind == ProjectionKind.MultiStream && resolver is not null)
            {
                var entityId = resolver.GetEntityId(se.Event);
                if (string.IsNullOrEmpty(entityId))
                    continue;

                var expectedEntityId = ExtractEntityIdFromInstanceId(instanceId, registration);
                if (!string.Equals(entityId, expectedEntityId, StringComparison.OrdinalIgnoreCase))
                    continue;
            }

            yield return se;
        }
    }

    // ── Raw event-stream construction (read strategy per projection kind) ──────

    private static IAsyncEnumerable<SequencedEvent> BuildRawStream(
        IEventStore eventStore,
        ProjectionRegistration registration,
        string instanceId,
        ProjectionCutoff? cutoff,
        CancellationToken ct)
    {
        switch (registration.Kind)
        {
            case ProjectionKind.SingleStream:
                // For TenantScoped projections the instanceId IS the full stream ID, so a direct
                // per-stream read is both correct and efficient.
                // For SystemGlobal/TenantGlobal projections the runner strips the tenant prefix
                // before saving to the view store, so instanceId may be only the bare aggregate ID
                // (e.g. "f60ef94-...") — a direct read would hit the wrong (non-existent) stream.
                // If the caller supplied a full stream ID (contains ':') we still do the direct read.
                if (IsFullStreamId(registration, instanceId))
                {
                    return (cutoff?.Kind) switch
                    {
                        CutoffKind.Version =>
                            eventStore.ReadStreamEnumerableAsync(
                                instanceId,
                                fromVersion: 0,
                                toVersion: cutoff!.Version,
                                cancellationToken: ct),

                        CutoffKind.Timestamp =>
                            eventStore.ReadStreamEnumerableAsync(
                                instanceId,
                                fromVersion: 0,
                                toTimestamp: cutoff!.Timestamp,
                                cancellationToken: ct),

                        _ => // AtSequence or no cutoff: read open-ended; caller breaks as needed.
                            eventStore.ReadStreamEnumerableAsync(
                                instanceId,
                                fromVersion: 0,
                                cancellationToken: ct)
                    };
                }
                // Bare aggregate ID — fall through to global scan; GetAppliedEventsAsync will
                // match via StreamIdParser.ExtractAggregateIdString.
                goto default;

            case ProjectionKind.Dcb when registration.DcbQueryTypes.Count > 0:
            case ProjectionKind.MultiStream when registration.DcbQueryTypes.Count > 0:
                return eventStore.ReadByQueryStreamAsync(
                    BuildFilteredQuery(registration),
                    fromSequencePosition: 0,
                    toSequencePosition: cutoff?.Kind == CutoffKind.Sequence ? cutoff.SequencePosition : null,
                    toTimestamp: cutoff?.Kind == CutoffKind.Timestamp ? cutoff.Timestamp : null,
                    cancellationToken: ct);

            default: // Global, untyped MultiStream
                return eventStore.ReadByQueryStreamAsync(
                    Query.All(),
                    fromSequencePosition: 0,
                    toSequencePosition: cutoff?.Kind == CutoffKind.Sequence ? cutoff.SequencePosition : null,
                    toTimestamp: cutoff?.Kind == CutoffKind.Timestamp ? cutoff.Timestamp : null,
                    cancellationToken: ct);
        }
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="instanceId"/> should be treated as a
    /// full stream ID suitable for a direct per-stream read, or <see langword="false"/> when it
    /// is a bare aggregate ID that requires a global scan with aggregate-ID matching.
    /// </summary>
    /// <remarks>
    /// <c>TenantScoped</c> projections always store the full stream ID as the instance key.
    /// <c>SystemGlobal</c> and <c>TenantGlobal</c> projections strip the tenant prefix, so the
    /// stored key is just the aggregate ID (no <c>':'</c>).  However, callers such as
    /// <see cref="AdHocProjectionBuilder"/> may pass the full stream ID directly — we detect
    /// this by checking whether <paramref name="instanceId"/> contains a colon.
    /// </remarks>
    private static bool IsFullStreamId(ProjectionRegistration registration, string instanceId)
        => registration.TenantScope == TenantScope.TenantScoped || instanceId.Contains(':');

    /// <summary>Builds a type-filtered DCB/MultiStream query from the registration's event type list.</summary>
    internal static Query BuildFilteredQuery(ProjectionRegistration registration) =>
        Query.FromItems(registration.DcbQueryTypes.Select(t => QueryItem.ByType(t)));

    /// <summary>
    /// Extracts the entity ID portion of a MultiStream view instance key.
    /// For TenantScoped the format is <c>"{tenantId}:{entityId}"</c>; otherwise just <c>"{entityId}"</c>.
    /// </summary>
    internal static string ExtractEntityIdFromInstanceId(
        string instanceId,
        ProjectionRegistration registration)
    {
        if (registration.TenantScope == TenantScope.TenantScoped)
        {
            var sep = instanceId.IndexOf(':');
            return sep >= 0 ? instanceId[(sep + 1)..] : instanceId;
        }

        return instanceId;
    }
}
