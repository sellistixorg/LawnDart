using Microsoft.Extensions.Logging;
using LawnDart.EventStore;

namespace LawnDart.Projections.Partitioning;

/// <summary>
/// Implementation of partitioning service using consistent hashing
/// </summary>
public class ConsistentHashPartitioningService : IPartitioningService
{
    private readonly ILogger<ConsistentHashPartitioningService>? _logger;
    
    public int NodeInstance { get; }
    public int TotalInstances { get; }

    public ConsistentHashPartitioningService(
        int nodeInstance, 
        int totalInstances,
        ILogger<ConsistentHashPartitioningService>? logger = null)
    {
        if (nodeInstance < 0 || nodeInstance >= totalInstances)
            throw new ArgumentException(
                $"NodeInstance must be 0-{totalInstances - 1}, but was {nodeInstance}");
        
        if (totalInstances <= 0)
            throw new ArgumentException("TotalInstances must be greater than 0");
        
        NodeInstance = nodeInstance;
        TotalInstances = totalInstances;
        _logger = logger;
        
        _logger?.LogInformation(
            "Partitioning service initialized: Node {NodeInstance} of {TotalInstances}", 
            NodeInstance, TotalInstances);
    }

    public bool OwnsStream(string streamId)
    {
        if (string.IsNullOrEmpty(streamId))
            throw new ArgumentNullException(nameof(streamId));
            
        return GetPartitionForStream(streamId) == NodeInstance;
    }

    public int GetPartitionForStream(string streamId)
    {
        if (string.IsNullOrEmpty(streamId))
            throw new ArgumentNullException(nameof(streamId));
        
        // Use shared deterministic hashing from Core library
        var hash = PartitionHashUtility.GetDeterministicHashCode(streamId);
        var partition = hash % TotalInstances;
        
        _logger?.LogTrace("Stream {StreamId} maps to partition {Partition} (hash: {Hash})", 
            streamId, partition, hash);
        
        return partition;
    }
    
    /// <summary>
    /// Compute a deterministic hash code that's consistent across all processes.
    /// Based on FNV-1a hash algorithm.
    /// </summary>
    /// <remarks>
    /// This method delegates to PartitionHashUtility for consistency across the codebase.
    /// </remarks>
    public static int GetDeterministicHashCode(string str)
    {
        return PartitionHashUtility.GetDeterministicHashCode(str);
    }

    public string GetPartitionInfo()
    {
        return $"Node {NodeInstance} of {TotalInstances} " +
               $"(handles ~{100.0 / TotalInstances:F1}% of streams)";
    }
}
