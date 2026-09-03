using Microsoft.Extensions.Logging;

namespace LawnDart.Projections.Partitioning;

/// <summary>
/// Single-node partitioning service that owns all streams (used when TotalInstances = 1)
/// </summary>
public class SingleNodePartitioningService : IPartitioningService
{
    private readonly ILogger<SingleNodePartitioningService>? _logger;
    
    public int NodeInstance => 0;
    public int TotalInstances => 1;

    public SingleNodePartitioningService(ILogger<SingleNodePartitioningService>? logger = null)
    {
        _logger = logger;
        _logger?.LogInformation("Single-node partitioning service initialized (owns all streams)");
    }

    public bool OwnsStream(string streamId)
    {
        // Single node owns all streams
        return true;
    }

    public int GetPartitionForStream(string streamId)
    {
        // All streams map to partition 0 (the only partition)
        return 0;
    }

    public string GetPartitionInfo()
    {
        return "Single Node (owns 100% of streams)";
    }
}
