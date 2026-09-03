namespace LawnDart.Projections.Partitioning;

/// <summary>
/// Service for determining partition ownership of streams across multiple nodes
/// </summary>
public interface IPartitioningService
{
    /// <summary>
    /// Determines if this node owns the given stream based on consistent hashing
    /// </summary>
    bool OwnsStream(string streamId);
    
    /// <summary>
    /// Gets the partition number for a stream (0 to TotalInstances-1)
    /// </summary>
    int GetPartitionForStream(string streamId);
    
    /// <summary>
    /// Current node instance number
    /// </summary>
    int NodeInstance { get; }
    
    /// <summary>
    /// Total number of instances in the cluster
    /// </summary>
    int TotalInstances { get; }
    
    /// <summary>
    /// Returns a string representation of this node's partition info
    /// </summary>
    string GetPartitionInfo();
}
