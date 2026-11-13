using LawnDart;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventStore;
using LawnDart.Messaging.InMemory;
using LawnDart.Messaging.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Host start fails with the context's event types when the outbox is on and nothing is declared.
/// The missing-declaration error comes out before a missing publisher.
/// </summary>
public sealed class OutboxHostStartTests
{
    private const string SqlConnection =
        "Server=localhost;Database=TestDb;User Id=sa;Password=Test123!;TrustServerCertificate=True;";

    private const string PostgresConnection =
        "Host=localhost;Username=lawndart_app;Password=lawndart_app;Database=lawndart";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InMemory_HostStart_NothingDeclared_NamesTheEventTypes(bool registerPublisher)
    {
        var text = await StartFailureAsync(services =>
        {
            if (registerPublisher)
            {
                services.AddInMemoryMessaging();
                services.AddMessageTransportOutboxPublisher();
            }

            services.AddBoundedContext("memory")
                .WithEventTypes(typeof(CounterCreated))
                .UseInMemory(o => o.EnableOutbox = true);
        });

        AssertDeclaresNothing(text, "memory");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SqlServer_HostStart_NothingDeclared_NamesTheEventTypes(bool registerPublisher)
    {
        var text = await StartFailureAsync(services =>
        {
            if (registerPublisher)
            {
                services.AddInMemoryMessaging();
                services.AddMessageTransportOutboxPublisher();
            }

            services.AddBoundedContext("sql")
                .WithEventTypes(typeof(CounterCreated))
                .UseSqlServer(o =>
                {
                    o.ConnectionString = SqlConnection;
                    o.EnableOutbox = true;
                });
        });

        AssertDeclaresNothing(text, "sql");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Postgres_HostStart_NothingDeclared_NamesTheEventTypes(bool registerPublisher)
    {
        var text = await StartFailureAsync(services =>
        {
            if (registerPublisher)
            {
                services.AddInMemoryMessaging();
                services.AddMessageTransportOutboxPublisher();
            }

            services.AddLawnDart();
            services.AddBoundedContext("postgres")
                .WithEventTypes(typeof(CounterCreated))
                .UsePostgres(o =>
                {
                    o.ConnectionString = PostgresConnection;
                    o.EnableOutbox = true;
                });
        });

        AssertDeclaresNothing(text, "postgres");
    }

    private static async Task<string> StartFailureAsync(Action<IServiceCollection> configure)
    {
        using var host = new HostBuilder()
            .ConfigureServices(configure)
            .Build();
        try
        {
            await host.StartAsync();
        }
        catch (Exception ex)
        {
            return Flatten(ex);
        }

        throw new InvalidOperationException("The host started.");
    }

    private static void AssertDeclaresNothing(string text, string contextName)
    {
        Assert.Contains($"'{contextName}' declares no published events", text, StringComparison.Ordinal);
        Assert.Contains("counter-created", text, StringComparison.Ordinal);
    }

    private static string Flatten(Exception exception)
    {
        var parts = new List<string>();
        for (var current = exception; current is not null; current = current.InnerException)
            parts.Add(current.Message);
        return string.Join(" ", parts);
    }
}
