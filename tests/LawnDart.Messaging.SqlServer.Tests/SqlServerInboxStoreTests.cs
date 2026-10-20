using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Messaging.SqlServer.Tests;

[Collection(SqlInboxCollection.Name)]
[Trait("Category", "Integration")]
public class SqlServerInboxStoreTests
{
    private readonly SqlInboxFixture _fixture;

    public SqlServerInboxStoreTests(SqlInboxFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task InitializeSchemaAsync_is_idempotent()
    {
        var store = CreateStore();
        await store.InitializeSchemaAsync();
        await store.InitializeSchemaAsync();

        Assert.False(await store.IsProcessedAsync("missing"));
    }







    [Fact]
    public async Task Incompatible_table_shape_names_drop_and_recreate()
    {
        var table = NewTable();
        await using (var connection = new SqlConnection(_fixture.ConnectionString))
        {
            await connection.OpenAsync();
            await using var create = new SqlCommand(
                $"CREATE TABLE [dbo].[{table}] (Id INT NOT NULL PRIMARY KEY);",
                connection);
            await create.ExecuteNonQueryAsync();
        }

        var store = CreateStore(table);
        var ex = await Assert.ThrowsAsync<IncompatibleInboxSchemaException>(() => store.InitializeSchemaAsync());
        Assert.Null(ex.FoundFormat);
        Assert.Equal(SqlServerInboxStore.CurrentSchemaFormat, ex.RequiredFormat);
        Assert.Contains("Drop and recreate", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InitializeSchemaAsync_accepts_schema_and_table_names_with_bracket_and_quote()
    {
        var store = new SqlServerInboxStore(
            Options.Create(new SqlServerInboxStoreOptions
            {
                ConnectionString = _fixture.ConnectionString,
                SchemaName = "a]'b",
                TableName = "In]'box"
            }),
            Options.Create(new MessagingOptions()));

        await store.InitializeSchemaAsync();
        await store.MarkProcessedAsync("odd", DateTimeOffset.UtcNow);
        Assert.True(await store.IsProcessedAsync("odd"));
    }

    [Fact]
    public async Task InitializeSqlInboxStoreAsync_creates_the_registered_table()
    {
        var table = NewTable();
        var services = new ServiceCollection();
        services.AddSqlInboxStore(_fixture.ConnectionString, options => options.TableName = table);
        using var provider = services.BuildServiceProvider();

        await provider.InitializeSqlInboxStoreAsync();

        var store = provider.GetRequiredService<SqlServerInboxStore>();
        await store.MarkProcessedAsync("from-di", DateTimeOffset.UtcNow);
        Assert.True(await store.IsProcessedAsync("from-di"));
    }


    private async Task<SqlServerInboxStore> CreateReadyStoreAsync(TimeSpan? window = null, TimeSpan? claimLease = null)
    {
        var store = CreateStore(window: window, claimLease: claimLease);
        await store.InitializeSchemaAsync();
        return store;
    }

    private SqlServerInboxStore CreateStore(string? table = null, TimeSpan? window = null, TimeSpan? claimLease = null)
    {
        table ??= NewTable();
        return new SqlServerInboxStore(
            Options.Create(new SqlServerInboxStoreOptions
            {
                ConnectionString = _fixture.ConnectionString,
                TableName = table
            }),
            Options.Create(new MessagingOptions
            {
                InboxDeduplicationWindow = window ?? TimeSpan.FromHours(24),
                InboxClaimLease = claimLease ?? TimeSpan.FromSeconds(30)
            }));
    }

    private static string NewTable() => "In" + Guid.NewGuid().ToString("N");

    private async Task<int> CountRowsAsync(SqlServerInboxStore store)
    {
        await using var connection = new SqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM {store.QualifiedTableName}", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private async Task<string> ReadSingleKeyAsync(SqlServerInboxStore store)
    {
        await using var connection = new SqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            $"SELECT MessageId FROM {store.QualifiedTableName}",
            connection);
        var value = await command.ExecuteScalarAsync();
        return Assert.IsType<string>(value);
    }

    private static async Task WaitForSubscriberAsync(InMemoryMessageTransport transport)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (transport.SubscriberCount<PingEvent>() == 0)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Reactor hosted service did not subscribe in time.");
            await Task.Delay(10);
        }
    }

    [EventTypeName("sql-inbox.ping")]
    private sealed record PingEvent(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class CountingReactor : IReactor<PingEvent>
    {
        public int CallCount { get; private set; }

        public Task<IEnumerable<ICommand>> ReactAsync(
            PingEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            return Task.FromResult<IEnumerable<ICommand>>([]);
        }
    }
}
