using System.Threading.Channels;
using LawnDart.Snapshots.Telemetry;
using Microsoft.Extensions.Options;

namespace LawnDart.EventSourcing.Snapshots;

internal sealed class SnapshotWriteQueue : ISnapshotWriteQueue
{
    private readonly Channel<SnapshotWriteItem> _channel;
    private long _dropped;
    private long _failures;

    public SnapshotWriteQueue(IOptions<SnapshotWriteOptions>? options = null)
    {
        var capacity = options?.Value.ChannelCapacity ?? 256;
        if (capacity < 1)
            capacity = 1;

        _channel = Channel.CreateBounded<SnapshotWriteItem>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
    }

    public long DroppedCount => Interlocked.Read(ref _dropped);

    public long FailureCount => Interlocked.Read(ref _failures);

    public bool IsDegraded => DroppedCount > 0 || FailureCount > 0;

    public bool TryEnqueue(SnapshotWriteItem item)
    {
        ArgumentNullException.ThrowIfNull(item);

        while (!_channel.Writer.TryWrite(item))
        {
            if (_channel.Reader.TryRead(out var dropped))
            {
                Interlocked.Increment(ref _dropped);
                SnapshotTelemetry.RecordWriteDrop(dropped.EntityType, dropped.Kind);
                continue;
            }

            if (_channel.Reader.Completion.IsCompleted)
                return false;
        }

        return true;
    }

    public IAsyncEnumerable<SnapshotWriteItem> ReadAllAsync(CancellationToken cancellationToken)
        => _channel.Reader.ReadAllAsync(cancellationToken);

    public void Complete() => _channel.Writer.TryComplete();

    public void RecordFailure() => Interlocked.Increment(ref _failures);
}
