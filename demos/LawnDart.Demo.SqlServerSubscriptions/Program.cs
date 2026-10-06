// SQL Server subscriptions: catch-up, then poll-backed live delivery.
//
// Create the database first. Schema init creates tables in schema subdemo.
// It does not create the database.
//
//   docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Your_password123" \
//     -p 1433:1433 -d mcr.microsoft.com/mssql/server:2022-latest
//
// DEMO ONLY: Your_password123 is a sample password.
//
//   export LAWNDART_SQL_CONNECTION="Server=localhost,1433;Database=LawnDartSubDemo;User Id=sa;Password=Your_password123;TrustServerCertificate=True"
//   dotnet run --project demos/LawnDart.Demo.SqlServerSubscriptions
//
// Or pass the connection string as argv[0].

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;

namespace LawnDart.Demo.SqlServerSubscriptions;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        var connectionString = args.ElementAtOrDefault(0)
            ?? Environment.GetEnvironmentVariable("LAWNDART_SQL_CONNECTION");

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.Error.WriteLine(
                "Set LAWNDART_SQL_CONNECTION or pass a connection string as argv[0].");
            Console.Error.WriteLine(
                "Create the database first. Schema init creates tables in schema subdemo. It does not create the database.");
            Console.Error.WriteLine(
                "Docker example and CREATE DATABASE are in demos/LawnDart.Demo.SqlServerSubscriptions/README.md.");
            return 1;
        }

        // Do not pass argv to the host. A connection string contains '=' and would be read as configuration.
        var builder = Host.CreateApplicationBuilder(Array.Empty<string>());

        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Logging.AddSimpleConsole(c => c.TimestampFormat = "[HH:mm:ss] ");

        builder.Services.AddLawnDart(o => o.RequireTenantId = false);
        builder.Services.AddBoundedContext("default")
            .UseSqlServer(o =>
            {
                o.ConnectionString = connectionString;
                o.RequireTenantId = false;
                o.SchemaName = "subdemo";
                o.SqlSubscriptionPollInterval = TimeSpan.FromMilliseconds(75);
                o.SqlSubscriptionBatchSize = 100;
            })
            .WithEventTypes<DemoEvent>();

        builder.Services.AddHostedService<DemoRunner>();

        var host = builder.Build();
        await host.RunAsync();
        return Environment.ExitCode;
    }
}

/// <summary>
/// Creates the schema, seeds three events, subscribes, appends one live event, and stops the host.
/// </summary>
internal sealed class DemoRunner : IHostedService
{
    private const string SubscriberId = "sql-demo";
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
        if (_store is not SqlServerEventStore sql)
        {
            Console.Error.WriteLine("SQL subscriptions did not resolve SqlServerEventStore.");
            Environment.ExitCode = 1;
            return;
        }

        await sql.InitializeSchemaAsync(cancellationToken);
        Console.WriteLine("Schema ready (schema=subdemo).");

        if (!ReferenceEquals(_store, _subscriptions))
        {
            Console.Error.WriteLine("IEventStore and IEventStoreSubscriptions resolved to different instances.");
            Environment.ExitCode = 1;
            return;
        }

        var headBefore = await _store.GetCurrentSequenceAsync(cancellationToken);
        for (var i = 0; i < 3; i++)
        {
            await _store.AppendAsync(
                StreamId,
                [new DemoEvent(Guid.NewGuid(), DateTime.UtcNow)],
                cancellationToken: cancellationToken);
        }

        var head = await _store.GetCurrentSequenceAsync(cancellationToken);
        // Subscribe at the first event seeded this run. Earlier rows stay in SQL and are not this run's catch-up.
        var fromSequence = headBefore + 1;
        Console.WriteLine($"Store head before seed = {headBefore}");
        Console.WriteLine($"Subscribing fromSequence = {fromSequence} (inclusive), seed head = {head}");

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);

        await using var handle = _subscriptions.Subscribe(
            SubscriberId,
            fromSequence,
            EventSubscriptionFilter.All(),
            linked.Token);

        // Live append after subscribe. Catch-up and live share one stream. Poll delivers the live row.
        var liveAppend = AppendLiveAsync(cancellationToken);

        var catchUp = 0;
        var live = 0;
        try
        {
            await foreach (var evt in handle.Events.ReadAllAsync(linked.Token))
            {
                var phase = evt.SequencePosition <= head ? "catch-up" : "live";
                Console.WriteLine($"  {phase} seq={evt.SequencePosition} stream={evt.StreamId}");
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

        if (catchUp != 3 || live != 1)
        {
            Console.Error.WriteLine($"Expected 3 catch-up and 1 live event. Saw catch-up={catchUp}, live={live}.");
            Environment.ExitCode = 1;
            return;
        }

        Console.WriteLine("Done (3 catch-up + 1 live via poll).");
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

/// <summary>Payload appended by the SQL Server subscriptions demo.</summary>
[EventTypeName("sql-subscription-demo-event")]
public sealed record DemoEvent(Guid Id, DateTime Timestamp) : IEvent;
