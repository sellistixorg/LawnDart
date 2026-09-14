using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.Snapshots;
using LawnDart.Snapshots.Telemetry;

namespace LawnDart.EventSourcing.Snapshots;

internal sealed class SnapshotWriteHostedService : IHostedService
{
    private static readonly MethodInfo SaveAggregate =
        typeof(ISnapshotStore).GetMethod(nameof(ISnapshotStore.SaveSnapshotAsync))!;

    private static readonly MethodInfo SaveDcb =
        typeof(IDcbSnapshotStore).GetMethod(nameof(IDcbSnapshotStore.SaveDcbSnapshotAsync))!;

    private readonly ISnapshotWriteQueue _queue;
    private readonly ILogger<SnapshotWriteHostedService> _logger;
    private Task? _pump;

    public SnapshotWriteHostedService(
        ISnapshotWriteQueue queue,
        ILogger<SnapshotWriteHostedService>? logger = null)
    {
        _queue = queue;
        _logger = logger ?? NullLogger<SnapshotWriteHostedService>.Instance;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _pump = PumpAsync();
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Complete();
        if (_pump is not null)
            await _pump.ConfigureAwait(false);
    }

    private async Task PumpAsync()
    {
        await foreach (var item in _queue.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
            await WriteAsync(item, CancellationToken.None).ConfigureAwait(false);
    }

    private async Task WriteAsync(SnapshotWriteItem item, CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var state = JsonSerializer.Deserialize(item.Payload, item.StateType, SnapshotJson.Options);
            if (state is null)
                throw new InvalidOperationException($"Snapshot payload for {item.Id} deserialized to null.");

            if (item.Kind == "dcb")
            {
                if (item.DcbStore is null)
                    throw new InvalidOperationException("DCB snapshot write is missing IDcbSnapshotStore.");

                var task = (Task)SaveDcb.MakeGenericMethod(item.StateType).Invoke(
                    item.DcbStore,
                    [item.Id, item.GlobalSequence, state, item.ConsistencyMarker, item.LoadTags, cancellationToken])!;
                await task.ConfigureAwait(false);
            }
            else
            {
                if (item.AggregateStore is null)
                    throw new InvalidOperationException("Aggregate snapshot write is missing ISnapshotStore.");

                var task = (Task)SaveAggregate.MakeGenericMethod(item.StateType).Invoke(
                    item.AggregateStore,
                    [item.Id, item.Version, item.GlobalSequence, state, cancellationToken])!;
                await task.ConfigureAwait(false);
            }

            sw.Stop();
            SnapshotTelemetry.RecordWrite(item.EntityType, item.Kind, sw.Elapsed);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            sw.Stop();
            _queue.RecordFailure();
            SnapshotTelemetry.RecordWriteFailure(item.EntityType, item.Kind);
            _logger.LogWarning(ex, "Snapshot write failed for {Kind} {Id}", item.Kind, item.Id);
        }
    }
}
