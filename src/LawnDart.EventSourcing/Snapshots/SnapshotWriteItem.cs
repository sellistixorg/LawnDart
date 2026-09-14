using LawnDart.Snapshots;

namespace LawnDart.EventSourcing.Snapshots;

internal sealed class SnapshotWriteItem
{
    public required string Kind { get; init; }
    public required string EntityType { get; init; }
    public required string Id { get; init; }
    public long Version { get; init; }
    public long GlobalSequence { get; init; }
    public required byte[] Payload { get; init; }
    public required Type StateType { get; init; }
    public ISnapshotStore? AggregateStore { get; init; }
    public IDcbSnapshotStore? DcbStore { get; init; }
    public byte[]? ConsistencyMarker { get; init; }
    public IReadOnlyList<string>? LoadTags { get; init; }
}
