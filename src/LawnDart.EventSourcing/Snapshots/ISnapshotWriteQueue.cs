namespace LawnDart.EventSourcing.Snapshots;

/// <summary>
/// Wait-free enqueue for captured snapshot payloads. The hosted consumer writes them.
/// </summary>
internal interface ISnapshotWriteQueue
{
    bool TryEnqueue(SnapshotWriteItem item);

    IAsyncEnumerable<SnapshotWriteItem> ReadAllAsync(CancellationToken cancellationToken);

    void Complete();

    long DroppedCount { get; }

    long FailureCount { get; }

    void RecordFailure();

    bool IsDegraded { get; }
}
