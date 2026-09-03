namespace LawnDart.EventStore;

/// <summary>
/// Represents a single query item that matches events by type and/or tags.
/// </summary>
public class QueryItem
{
    /// <summary>
    /// Event types to match. An event matches if its type matches any of these types.
    /// </summary>
    public IReadOnlyList<string>? Types { get; init; }

    /// <summary>
    /// Tags that must all be present. An event matches if it contains all of these tags.
    /// </summary>
    public IReadOnlyList<string>? Tags { get; init; }

    /// <summary>
    /// Partition filter for multi-node deployments. Filters events by StreamId hash to enable
    /// database-level partition filtering.
    /// </summary>
    public PartitionFilter? PartitionFilter { get; init; }

    /// <summary>
    /// Creates a query item that matches events by type.
    /// </summary>
    public static QueryItem ByType(params string[] types) => new() { Types = types };

    /// <summary>
    /// Creates a query item that matches events by tags.
    /// </summary>
    public static QueryItem ByTags(params string[] tags) => new() { Tags = tags };

    /// <summary>
    /// Creates a query item that matches events by both type and tags.
    /// </summary>
    public static QueryItem ByTypeAndTags(string[] types, string[] tags) => new() { Types = types, Tags = tags };

    /// <summary>
    /// Creates a query item that matches events by partition (for multi-node deployments).
    /// </summary>
    public static QueryItem ByPartition(int nodeInstance, int totalInstances) 
        => new() { PartitionFilter = new PartitionFilter(nodeInstance, totalInstances) };
}

/// <summary>
/// Partition filter for multi-node event store queries.
/// </summary>
/// <param name="NodeInstance">The node instance number (0-based)</param>
/// <param name="TotalInstances">The total number of instances in the cluster</param>
public record PartitionFilter(int NodeInstance, int TotalInstances);


