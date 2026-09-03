namespace LawnDart.EventStore;

/// <summary>
/// Represents a query for filtering events by type and/or tags.
/// Query items are combined with OR logic - an event matches if it matches any query item.
/// </summary>
public class Query
{
    /// <summary>
    /// Query items. An event matches the query if it matches any of these items.
    /// </summary>
    public IReadOnlyList<QueryItem> Items { get; init; }

    private Query(IReadOnlyList<QueryItem> items)
    {
        Items = items ?? throw new ArgumentNullException(nameof(items));
    }

    [System.Text.Json.Serialization.JsonConstructor]
    private Query() : this(Array.Empty<QueryItem>()) { }

    /// <summary>
    /// Creates a query that matches all events.
    /// </summary>
    public static Query All() => new(Array.Empty<QueryItem>());

    /// <summary>
    /// Creates a query from one or more query items.
    /// </summary>
    public static Query FromItems(params QueryItem[] items)
    {
        if (items == null || items.Length == 0)
            throw new ArgumentException("At least one query item is required, or use Query.All()", nameof(items));

        return new Query(items);
    }

    /// <summary>
    /// Creates a query from a collection of query items.
    /// </summary>
    public static Query FromItems(IEnumerable<QueryItem> items)
    {
        var itemsList = items?.ToList() ?? throw new ArgumentNullException(nameof(items));
        if (itemsList.Count == 0)
            throw new ArgumentException("At least one query item is required, or use Query.All()", nameof(items));

        return new Query(itemsList);
    }
}


