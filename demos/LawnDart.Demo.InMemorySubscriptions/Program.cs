// InMemory subscriptions: catch-up, then live, with a file-backed client cursor.
//
// Run:
//   dotnet run --project demos/LawnDart.Demo.InMemorySubscriptions
//
// Re-run resumes from subscribe-checkpoint.txt. A new process starts with an
// empty InMemory store, so the demo seeds events again. A cursor outside
// [0, head), or one left by a partial write, is reset before subscribe.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.EventSourcing;
using LawnDart.EventStore;

namespace LawnDart.Demo.InMemorySubscriptions;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Logging.AddSimpleConsole(c => c.TimestampFormat = "[HH:mm:ss] ");

        builder.Services.AddLawnDart(o => o.RequireTenantId = false);
        builder.Services.AddBoundedContext("default")
            .UseInMemory()
            .WithEventTypes<DemoEvent>();

        builder.Services.AddHostedService<DemoRunner>();

        var host = builder.Build();
        await host.RunAsync();
    }
}

/// <summary>
/// Seeds the store, subscribes, writes a client checkpoint, and stops the host.
/// </summary>
internal sealed class DemoRunner : IHostedService
{
    private const string SubscriberId = "demo-file-checkpoint";
    private const string StreamId = "demo-stream";

    private readonly IEventStore _store;
    private readonly IEventStoreSubscriptions _subscriptions;
    private readonly IHostApplicationLifetime _lifetime;

    public DemoRunner(
        IEventStore store,
        IEventStoreSubscriptions subscriptions,
        IHostApplicationLifetime lifetime)
    {
        _store = store;
        _subscriptions = subscriptions;
        _lifetime = lifetime;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            await RunAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex);
            Environment.ExitCode = 1;
        }
        finally
        {
            _lifetime.StopApplication();
        }
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        var checkpointPath = Path.Combine(AppContext.BaseDirectory, "subscribe-checkpoint.txt");
        long lastApplied = File.Exists(checkpointPath)
            && long.TryParse(await File.ReadAllTextAsync(checkpointPath, cancellationToken), out var parsed)
            ? parsed
            : 0;

        Console.WriteLine($"Client cursor (lastApplied) = {lastApplied}");

        if (!ReferenceEquals(_store, _subscriptions))
        {
            Console.Error.WriteLine("IEventStore and IEventStoreSubscriptions resolved to different instances.");
            Environment.ExitCode = 1;
            return;
        }

        // Seed a small history every run. InMemory does not keep events across processes.
        for (var i = 0; i < 3; i++)
        {
            await _store.AppendAsync(
                StreamId,
                [new DemoEvent(Guid.NewGuid(), DateTime.UtcNow)],
                cancellationToken: cancellationToken);
        }

        var head = await _store.GetCurrentSequenceAsync(cancellationToken);
        // [0, head) still replays every seeded event only when the cursor is 0.
        // A previous process can leave lastApplied ahead of head, or a partial
        // write inside that range. Either one skips events and the loop waits.
        var inRange = lastApplied >= 0 && lastApplied < head;
        if (!inRange)
        {
            Console.WriteLine(
                $"Checkpoint ({lastApplied}) is outside [0, {head}); resetting cursor (InMemory is empty each process).");
            lastApplied = 0;
        }
        else if (lastApplied > 0)
        {
            Console.WriteLine(
                $"Checkpoint ({lastApplied}) would skip seeded events under head {head}; resetting cursor (InMemory is empty each process).");
            lastApplied = 0;
        }

        var fromSequence = lastApplied <= 0 ? 1 : lastApplied + 1;
        Console.WriteLine($"Subscribing fromSequence = {fromSequence} (inclusive)");

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);

        await using var handle = _subscriptions.Subscribe(
            SubscriberId,
            fromSequence,
            EventSubscriptionFilter.All(),
            linked.Token);

        // Live append after subscribe. Catch-up and live share one stream. There is no separate go-live call.
        var liveAppend = AppendLiveAsync(cancellationToken);

        var catchUp = 0;
        var live = 0;
        try
        {
            await foreach (var evt in handle.Events.ReadAllAsync(linked.Token))
            {
                var phase = evt.SequencePosition <= head ? "catch-up" : "live";
                Console.WriteLine($"  {phase} seq={evt.SequencePosition} stream={evt.StreamId}");
                lastApplied = evt.SequencePosition;
                await File.WriteAllTextAsync(checkpointPath, lastApplied.ToString(), linked.Token);
                if (evt.SequencePosition <= head)
                    catchUp++;
                else
                    live++;

                if (catchUp >= 3 && live >= 1)
                    break;
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            Console.Error.WriteLine("Timed out waiting for 3 catch-up events and 1 live event.");
            Environment.ExitCode = 1;
            return;
        }

        await liveAppend;

        Console.WriteLine($"Done. Checkpoint written to {checkpointPath} (lastApplied={lastApplied}).");

        if (catchUp != 3 || live != 1)
        {
            Console.Error.WriteLine($"Expected 3 catch-up and 1 live event. Saw catch-up={catchUp}, live={live}.");
            Environment.ExitCode = 1;
        }
    }

    private async Task AppendLiveAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(200, cancellationToken);
        await _store.AppendAsync(
            StreamId,
            [new DemoEvent(Guid.NewGuid(), DateTime.UtcNow)],
            cancellationToken: cancellationToken);
    }
}

/// <summary>Payload appended by the subscriptions demo.</summary>
[EventTypeName("subscription-demo-event")]
public sealed record DemoEvent(Guid Id, DateTime Timestamp) : IEvent;
