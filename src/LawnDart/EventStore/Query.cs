namespace LawnDart.EventStore;

/// <summary>
/// Represents a query for filtering events by type and/or tags.
/// Query items are combined with OR logic; an event matches if it matches any query item.
/// </summary>
public class Query
{
    /// <summary>
    /// Query items. An event matches the query if it matches any of these items.
    /// </summary>
    public IReadOnlyList<QueryItem> Items { get; init; }

    /// <summary>
    /// Optional tenant scope for the query. Set via <see cref="ForTenant"/>.
    /// </summary>
    public string? TenantId { get; init; }

    /// <summary>
    /// When true, the query reads across every tenant in a shared store.
    /// Set only by <see cref="ForAllTenants()"/> and its overloads.
    /// </summary>
    public bool AllTenants { get; init; }

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

    /// <summary>
    /// Returns a copy of this query scoped to <paramref name="tenantId"/>.
    /// </summary>
    public Query ForTenant(string tenantId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        if (!string.Equals(tenantId.Trim(), tenantId, StringComparison.Ordinal))
            throw new ArgumentException("Tenant id must not have leading or trailing whitespace.", nameof(tenantId));
        if (tenantId.Contains(':', StringComparison.Ordinal))
            throw new ArgumentException("Tenant id must not contain ':'.", nameof(tenantId));
        if (AllTenants)
            throw new InvalidOperationException("Cannot scope an all-tenants query to a single tenant.");

        return new Query(Items) { TenantId = tenantId };
    }

    /// <summary>
    /// Creates an explicit cross-tenant query matching all events across every tenant.
    /// </summary>
    public static Query ForAllTenants() => new(Array.Empty<QueryItem>()) { AllTenants = true };

    /// <summary>
    /// Creates an explicit cross-tenant query from one or more query items.
    /// </summary>
    public static Query ForAllTenants(params QueryItem[] items)
    {
        if (items == null || items.Length == 0)
            throw new ArgumentException("At least one query item is required, or use Query.ForAllTenants()", nameof(items));

        return new Query(items) { AllTenants = true };
    }

    /// <summary>
    /// Creates an explicit cross-tenant query from a collection of query items.
    /// </summary>
    public static Query ForAllTenants(IEnumerable<QueryItem> items)
    {
        var itemsList = items?.ToList() ?? throw new ArgumentNullException(nameof(items));
        if (itemsList.Count == 0)
            throw new ArgumentException("At least one query item is required, or use Query.ForAllTenants()", nameof(items));

        return new Query(itemsList) { AllTenants = true };
    }
}
