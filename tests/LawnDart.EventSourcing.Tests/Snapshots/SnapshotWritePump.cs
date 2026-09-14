using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.EventSourcing.Snapshots;

namespace LawnDart.EventSourcing.Tests.Snapshots;

internal sealed class SnapshotWritePump : IAsyncDisposable
{
    public SnapshotWriteQueue Queue { get; }
    private readonly SnapshotWriteHostedService _hosted;

    public SnapshotWritePump(int capacity = 256)
    {
        Queue = new SnapshotWriteQueue(Options.Create(new SnapshotWriteOptions { ChannelCapacity = capacity }));
        _hosted = new SnapshotWriteHostedService(Queue, NullLogger<SnapshotWriteHostedService>.Instance);
    }

    public Task StartAsync() => _hosted.StartAsync(CancellationToken.None);

    public async ValueTask DisposeAsync() =>
        await _hosted.StopAsync(CancellationToken.None).ConfigureAwait(false);
}
