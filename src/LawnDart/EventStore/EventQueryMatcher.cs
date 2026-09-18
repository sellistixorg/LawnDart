namespace LawnDart.EventStore;

/// <summary>
/// Shared DCB <see cref="Query"/> matching for typed and log reads / subscriptions.
/// Backends should reuse this helper (or equivalent) so subscription <see cref="EventSubscriptionFilter.ForQuery"/>
/// stays aligned with <c>ReadByQuery*</c> semantics. Prefer <see cref="Matches(RecordedEvent, Query)"/>
/// on the log so a filter does not hydrate a CLR type.
/// </summary>
public static class EventQueryMatcher
{
    /// <summary>
    /// Returns whether <paramref name="sequencedEvent"/> matches <paramref name="query"/>.
    /// Empty <see cref="Query.Items"/> (i.e. <see cref="Query.All"/>) matches everything.
    /// Otherwise an event matches if it matches any query item (OR across items).
    /// </summary>
    public static bool Matches(SequencedEvent sequencedEvent, Query query, IEventTypeCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(sequencedEvent);
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(catalog);

        return MatchesCore(
            sequencedEvent.StreamId,
            ResolveTypeName(sequencedEvent.Event, catalog),
            sequencedEvent.Tags,
            query);
    }

    /// <summary>
    /// Returns whether <paramref name="recordedEvent"/> matches <paramref name="query"/>
    /// using the stored family token. Does not hydrate a CLR type.
    /// </summary>
    public static bool Matches(RecordedEvent recordedEvent, Query query)
    {
        ArgumentNullException.ThrowIfNull(recordedEvent);
        ArgumentNullException.ThrowIfNull(query);

        return MatchesCore(
            recordedEvent.StreamId,
            recordedEvent.EventType,
            recordedEvent.Tags,
            query);
    }

    private static bool MatchesCore(
        string streamId,
        string eventTypeName,
        IReadOnlyList<string> tags,
        Query query)
    {
        if (query.Items.Count == 0)
            return true;

        foreach (var item in query.Items)
        {
            if (MatchesItem(streamId, eventTypeName, tags, item))
                return true;
        }

        return false;
    }

    private static bool MatchesItem(
        string streamId,
        string eventTypeName,
        IReadOnlyList<string> tags,
        QueryItem item)
    {
        if (item.PartitionFilter != null)
        {
            var pf = item.PartitionFilter;
            var hash = PartitionHashUtility.GetDeterministicHashCode(streamId);
            var partition = hash % pf.TotalInstances;
            if (partition != pf.NodeInstance)
                return false;
        }

        if (item.Types is { Count: > 0 } && !item.Types.Contains(eventTypeName))
            return false;

        if (item.Tags is { Count: > 0 })
        {
            foreach (var requiredTag in item.Tags)
            {
                if (!tags.Contains(requiredTag))
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Prefer <see cref="IRawEvent.TypeName"/> (the stored family token).
    /// <see cref="IEventTypeCatalog.GetName"/> would otherwise see the
    /// wrapper CLR name and drop typed query / subscribe matches.
    /// </summary>
    private static string ResolveTypeName(IEvent @event, IEventTypeCatalog catalog) =>
        @event is IRawEvent raw && !string.IsNullOrWhiteSpace(raw.TypeName)
            ? raw.TypeName
            : catalog.GetName(@event.GetType());
}
