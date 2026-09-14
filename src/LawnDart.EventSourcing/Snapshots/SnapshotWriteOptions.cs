namespace LawnDart.EventSourcing.Snapshots;

/// <summary>
/// Capacity and observability for the off-path snapshot write channel.
/// </summary>
public sealed class SnapshotWriteOptions
{
    /// <summary>
    /// Maximum pending snapshot writes. When full, the oldest pending write is
    /// dropped so enqueue stays wait-free. Default 256.
    /// </summary>
    public int ChannelCapacity { get; set; } = 256;
}
