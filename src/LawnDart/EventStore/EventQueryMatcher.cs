namespace LawnDart.EventStore;

/// <summary>
/// Shared DCB <see cref="Query"/> matching for event-store reads and portable subscriptions.
/// Backends should reuse this helper (or equivalent) so subscription <see cref="EventSubscriptionFilter.ForQuery"/>
/// stays aligned with <c>ReadByQuery*</c> semantics.
/// </summary>
public static class EventQueryMatcher
{
    /// <summary>
    /// Returns whether <paramref name="sequencedEvent"/> matches <paramref name="query"/>.
    /// Empty <see cref="Query.Items"/> (i.e. <see cref="Query.All"/>) matches everything.
    /// Otherwise an event matches if it matches any query item (OR across items).
    /// </summary>
    public static bool Matches(SequencedEvent sequencedEvent, Query query)
    {
        ArgumentNullException.ThrowIfNull(sequencedEvent);
        ArgumentNullException.ThrowIfNull(query);

        if (query.Items.Count == 0)
            return true;

        foreach (var item in query.Items)
        {
            if (MatchesItem(sequencedEvent, item))
                return true;
        }

        return false;
    }

    private static bool MatchesItem(SequencedEvent sequencedEvent, QueryItem item)
    {
        if (item.PartitionFilter != null)
        {
            var pf = item.PartitionFilter;
            var hash = PartitionHashUtility.GetDeterministicHashCode(sequencedEvent.StreamId);
            var partition = hash % pf.TotalInstances;
            if (partition != pf.NodeInstance)
                return false;
        }

        if (item.Types is { Count: > 0 })
        {
            var eventTypeName = ResolveTypeName(sequencedEvent.Event);
            if (!item.Types.Contains(eventTypeName))
                return false;
        }

        if (item.Tags is { Count: > 0 })
        {
            foreach (var requiredTag in item.Tags)
            {
                if (!sequencedEvent.Tags.Contains(requiredTag))
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Prefer the persisted type name on <see cref="IRawEvent"/> (gRPC <c>OpaqueEvent</c> /
    /// unknown types). CLR <see cref="EventTypeNameResolver"/> would otherwise see
    /// <c>OpaqueEvent</c> and drop every typed Subscribe match.
    /// </summary>
    private static string ResolveTypeName(IEvent @event) =>
        @event is IRawEvent raw && !string.IsNullOrWhiteSpace(raw.TypeName)
            ? raw.TypeName
            : EventTypeNameResolver.GetName(@event.GetType());
}
