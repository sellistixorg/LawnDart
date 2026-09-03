using System.Text.Json;
using System.Text.Json.Nodes;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.TimeTravelQuery;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Debug;

/// <summary>
/// Provides debug-oriented lightweight projection inspection helpers such as instance listing,
/// applied-event timelines, point-in-time replay by applied index, and structural state diffs.
/// </summary>
public sealed class ProjectionTimelineService(
    IEventStore eventStore,
    IViewStore viewStore,
    IReadOnlyList<ProjectionRegistration> registrations)
{
    private readonly ProjectionRegistrationCatalog _catalog = new(registrations);
    private readonly AdHocProjectionBuilder _builder = new(eventStore, registrations);

    /// <summary>
    /// Lists stored projection instance IDs with optional tenant filtering and paging.
    /// </summary>
    public async Task<ProjectionInstancePage> GetInstancePageAsync(
        string projectionName,
        string? tenantId,
        int page,
        int pageSize,
        CancellationToken cancellationToken = default,
        int? version = null)
    {
        var registration = _catalog.Resolve(projectionName, version);
        var stored = await viewStore.GetViewsByTypeAsync(registration.StorageKey, cancellationToken);

        var filtered = stored
            .Select(v => v.InstanceId)
            .Where(id => string.IsNullOrEmpty(tenantId) || id.StartsWith($"{tenantId}:", StringComparison.OrdinalIgnoreCase))
            .OrderBy(id => id, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var safePage = Math.Max(0, page);
        var safePageSize = Math.Max(1, pageSize);

        return new ProjectionInstancePage
        {
            LogicalName = registration.LogicalName,
            ProjectionVersion = registration.Version,
            ProjectionStorageKey = registration.StorageKey,
            IsLatest = registration.IsLatest,
            LatestResolutionMode = registration.LatestResolutionMode.ToString(),
            TenantId = tenantId ?? string.Empty,
            Items = filtered
                .Skip(safePage * safePageSize)
                .Take(safePageSize)
                .ToList(),
            Page = safePage,
            PageSize = safePageSize,
            TotalFiltered = filtered.Count
        };
    }

    /// <summary>
    /// Returns a paged slice of the applied-event timeline for a projection instance.
    /// </summary>
    public async Task<ProjectionTimeline> GetTimelinePageAsync(
        string projectionName,
        string instanceId,
        int fromIndex,
        int pageSize,
        CancellationToken cancellationToken = default,
        int? version = null)
    {
        var registration = _catalog.Resolve(projectionName, version);
        var (_, _, _, _, entityResolver) = _builder.CreateReplayContext(projectionName, version);

        var safeFromIndex = Math.Max(0, fromIndex);
        var safePageSize = Math.Max(1, pageSize);
        var entries = new List<AppliedEventEntry>(safePageSize);
        long appliedIndex = 0;

        await foreach (var se in ProjectionEventFilter.GetAppliedEventsAsync(
            eventStore,
            registration,
            instanceId,
            cutoff: null,
            entityResolver,
            cancellationToken))
        {
            if (appliedIndex >= safeFromIndex && entries.Count < safePageSize)
                entries.Add(ToAppliedEventEntry(appliedIndex, se));

            appliedIndex++;
        }

        return new ProjectionTimeline
        {
            LogicalName = registration.LogicalName,
            ProjectionVersion = registration.Version,
            ProjectionStorageKey = registration.StorageKey,
            IsLatest = registration.IsLatest,
            LatestResolutionMode = registration.LatestResolutionMode.ToString(),
            InstanceId = instanceId,
            Events = entries,
            FromIndex = safeFromIndex,
            PageSize = safePageSize,
            TotalApplied = appliedIndex
        };
    }

    /// <summary>
    /// Replays a projection instance through a specific applied-event count.
    /// </summary>
    public async Task<TimeTravelResult> BuildAtAppliedIndexAsync(
        string projectionName,
        string instanceId,
        long appliedCount,
        CancellationToken cancellationToken = default,
        int? version = null)
    {
        var (registration, handler, processEvent, getView, entityResolver) =
            _builder.CreateReplayContext(projectionName, version);

        if (appliedCount <= 0)
            return CreateReplayResult(registration, instanceId, getView(handler), 0, null, null, null, "applied index <= 0");

        long eventsApplied = 0;
        long? finalSequencePosition = null;
        long? finalStreamVersion = null;
        DateTime? finalEventTimestamp = null;

        await foreach (var se in ProjectionEventFilter.GetAppliedEventsAsync(
            eventStore,
            registration,
            instanceId,
            cutoff: null,
            entityResolver,
            cancellationToken))
        {
            if (eventsApplied >= appliedCount)
                break;

            processEvent(handler, se.Event, se.SequencePosition);
            eventsApplied++;
            finalSequencePosition = se.SequencePosition;
            finalStreamVersion = se.Version;
            finalEventTimestamp = se.Metadata.Timestamp;
        }

        return CreateReplayResult(
            registration,
            instanceId,
            getView(handler),
            eventsApplied,
            finalSequencePosition,
            finalStreamVersion,
            finalEventTimestamp,
            $"applied index < {appliedCount}");
    }

    /// <summary>
    /// Builds the state at a target applied index and the immediately previous state for diffing.
    /// </summary>
    public async Task<(TimeTravelResult AtTarget, TimeTravelResult? AtPrevious, AppliedEventEntry? TargetEvent)> BuildStepAsync(
        string projectionName,
        string instanceId,
        long targetAppliedIndex,
        CancellationToken cancellationToken = default,
        int? version = null)
    {
        if (targetAppliedIndex < 0)
        {
            var empty = await BuildAtAppliedIndexAsync(
                projectionName,
                instanceId,
                appliedCount: 0,
                cancellationToken,
                version);
            return (empty, null, null);
        }

        var atTarget = await BuildAtAppliedIndexAsync(
            projectionName,
            instanceId,
            appliedCount: targetAppliedIndex + 1,
            cancellationToken,
            version);

        var atPrevious = await BuildAtAppliedIndexAsync(
            projectionName,
            instanceId,
            appliedCount: targetAppliedIndex,
            cancellationToken,
            version);

        var timeline = await GetTimelinePageAsync(
            projectionName,
            instanceId,
            fromIndex: (int)targetAppliedIndex,
            pageSize: 1,
            cancellationToken,
            version);

        return (atTarget, atPrevious, timeline.Events.FirstOrDefault());
    }

    /// <summary>
    /// Computes a structural diff between two serialized JSON view states.
    /// </summary>
    public static ViewStateDiff ComputeDiff(string previousJson, string nextJson)
    {
        JsonNode? previous = string.IsNullOrWhiteSpace(previousJson) ? null : JsonNode.Parse(previousJson);
        JsonNode? next = string.IsNullOrWhiteSpace(nextJson) ? null : JsonNode.Parse(nextJson);

        var added = new List<string>();
        var removed = new List<string>();
        var changed = new List<ChangedProperty>();

        CompareNodes(previous, next, path: string.Empty, added, removed, changed);

        return new ViewStateDiff
        {
            AddedPaths = added,
            RemovedPaths = removed,
            ChangedProperties = changed
        };
    }

    private static void CompareNodes(
        JsonNode? previous,
        JsonNode? next,
        string path,
        ICollection<string> added,
        ICollection<string> removed,
        ICollection<ChangedProperty> changed)
    {
        if (previous is null && next is null)
            return;

        if (previous is null)
        {
            added.Add(ToReportedPath(path));
            return;
        }

        if (next is null)
        {
            removed.Add(ToReportedPath(path));
            return;
        }

        if (previous is JsonObject previousObject && next is JsonObject nextObject)
        {
            foreach (var key in previousObject.Select(kvp => kvp.Key).Union(nextObject.Select(kvp => kvp.Key), StringComparer.Ordinal))
            {
                var childPath = AppendObjectPath(path, key);
                previousObject.TryGetPropertyValue(key, out var previousChild);
                nextObject.TryGetPropertyValue(key, out var nextChild);
                CompareNodes(previousChild, nextChild, childPath, added, removed, changed);
            }
            return;
        }

        if (previous is JsonArray previousArray && next is JsonArray nextArray)
        {
            CompareArrays(previousArray, nextArray, path, added, removed, changed);
            return;
        }

        if (previous is JsonArray || next is JsonArray)
        {
            if (!JsonNode.DeepEquals(previous, next))
                changed.Add(new ChangedProperty(ToReportedPath(path), ToJsonElement(previous), ToJsonElement(next)));
            return;
        }

        if (!JsonNode.DeepEquals(previous, next))
            changed.Add(new ChangedProperty(ToReportedPath(path), ToJsonElement(previous), ToJsonElement(next)));
    }

    private static void CompareArrays(
        JsonArray previous,
        JsonArray next,
        string path,
        ICollection<string> added,
        ICollection<string> removed,
        ICollection<ChangedProperty> changed)
    {
        var overlap = Math.Min(previous.Count, next.Count);
        for (var i = 0; i < overlap; i++)
        {
            CompareNodes(
                previous[i],
                next[i],
                AppendArrayPath(path, i),
                added,
                removed,
                changed);
        }

        for (var i = overlap; i < next.Count; i++)
            added.Add(AppendArrayPath(path, i));

        for (var i = overlap; i < previous.Count; i++)
            removed.Add(AppendArrayPath(path, i));
    }

    private static string AppendObjectPath(string path, string key) =>
        string.IsNullOrEmpty(path) ? key : $"{path}.{key}";

    private static string AppendArrayPath(string path, int index) =>
        $"{path}[{index}]";

    private static string ToReportedPath(string path) =>
        string.IsNullOrEmpty(path) ? "(root)" : path;
    
    private static string SerializeView(object view) =>
        JsonSerializer.Serialize(view, view.GetType(), ProjectionViewJson.Write);

    private static string CreateEmptyViewJson(object view)
    {
        var json = SerializeView(view);
        return string.IsNullOrWhiteSpace(json) ? "{}" : json;
    }

    private static AppliedEventEntry ToAppliedEventEntry(long appliedIndex, SequencedEvent se) =>
        new()
        {
            AppliedIndex = appliedIndex,
            SequencePosition = se.SequencePosition,
            StreamId = se.StreamId,
            Version = se.Version,
            Timestamp = se.Metadata.Timestamp,
            EventType = se.Event.GetType().Name,
            EventJson = ToJsonElement(JsonSerializer.SerializeToNode(se.Event, se.Event.GetType())),
            EventId = se.Metadata.EventId,
            UserId = se.Metadata.UserId,
            UserName = se.Metadata.UserName,
            TenantId = se.Metadata.TenantId,
            CorrelationId = se.Metadata.CorrelationId,
            CausationId = se.Metadata.CausationId,
            CommitTimestamp = se.Metadata.CommitTimestamp,
            IpAddress = se.Metadata.IpAddress,
            Tags = se.Tags.ToArray()
        };

    private static JsonElement? ToJsonElement(JsonNode? node)
    {
        if (node is null)
            return null;

        using var doc = JsonDocument.Parse(node.ToJsonString());
        return doc.RootElement.Clone();
    }

    private static TimeTravelResult CreateReplayResult(
        ProjectionRegistration registration,
        string instanceId,
        object view,
        long eventsApplied,
        long? finalSequencePosition,
        long? finalStreamVersion,
        DateTime? finalEventTimestamp,
        string requestedCutoff)
    {
        var viewJson = eventsApplied == 0
            ? CreateEmptyViewJson(view)
            : SerializeView(view);

        return new TimeTravelResult
        {
            LogicalName = registration.LogicalName,
            ProjectionVersion = registration.Version,
            ProjectionStorageKey = registration.StorageKey,
            IsLatest = registration.IsLatest,
            LatestResolutionMode = registration.LatestResolutionMode.ToString(),
            InstanceId = instanceId,
            ViewJson = viewJson,
            EventsApplied = eventsApplied,
            FinalSequencePosition = finalSequencePosition,
            FinalStreamVersion = finalStreamVersion,
            FinalEventTimestamp = finalEventTimestamp,
            RequestedCutoff = requestedCutoff,
            ProjectionKind = registration.Kind.ToString()
        };
    }
}
