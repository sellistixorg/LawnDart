namespace LawnDart.Projections;

/// <summary>
/// Represents a checkpoint for a projection type on a specific node.
/// Tracks the last processed sequence position for incremental event processing.
/// </summary>
public class ProjectionCheckpoint
{
    /// <summary>
    /// The projection type name (e.g., "OrderSummary", "CartView")
    /// </summary>
    public string ProjectionType { get; set; } = string.Empty;
    
    /// <summary>
    /// The node instance identifier (0-based). Used for multi-node partitioning.
    /// </summary>
    public int NodeId { get; set; }
    
    /// <summary>
    /// The last sequence position that was successfully processed.
    /// </summary>
    public long LastSequencePosition { get; set; }
    
    /// <summary>
    /// The timestamp when this checkpoint was last updated.
    /// </summary>
    public DateTime LastUpdated { get; set; }
    
    /// <summary>
    /// Total number of events processed by this projection on this node.
    /// </summary>
    public long TotalEventsProcessed { get; set; }
}
