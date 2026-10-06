using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart;
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
    public async Task Unknown_id_is_not_processed()
    {
        var store = await CreateReadyStoreAsync();
        Assert.False(await store.IsProcessedAsync("never-seen"));
    }

    [Fact]
    public async Task Mark_then_lookup_is_true_and_a_second_mark_does_not_throw()
    {
        var store = await CreateReadyStoreAsync();
        await store.MarkProcessedAsync("msg-1", DateTimeOffset.UtcNow);
        Assert.True(await store.IsProcessedAsync("msg-1"));

        var ex = await Record.ExceptionAsync(() => store.MarkProcessedAsync("msg-1", DateTimeOffset.UtcNow));
        Assert.Null(ex);
        Assert.Equal(1, await CountRowsAsync(store));
    }

    [Fact]
    public async Task Expired_row_is_not_processed_and_purge_removes_only_expired_rows()
    {
        var store = await CreateReadyStoreAsync(TimeSpan.FromHours(1));
        await store.MarkProcessedAsync("fresh", DateTimeOffset.UtcNow);
        await store.MarkProcessedAsync("stale", DateTimeOffset.UtcNow - TimeSpan.FromHours(2));

        Assert.True(await store.IsProcessedAsync("fresh"));
        Assert.False(await store.IsProcessedAsync("stale"));

        var purged = await store.PurgeExpiredAsync();
        Assert.Equal(1, purged);
        Assert.True(await store.IsProcessedAsync("fresh"));
        Assert.False(await store.IsProcessedAsync("stale"));
        Assert.Equal(1, await CountRowsAsync(store));
    }

    [Fact]
    public async Task A_new_store_instance_sees_prior_marks()
    {
        var table = NewTable();
        var first = CreateStore(table);
        await first.InitializeSchemaAsync();
        await first.MarkProcessedAsync("durable", DateTimeOffset.UtcNow);

        var second = CreateStore(table);
        Assert.True(await second.IsProcessedAsync("durable"));
    }

    [Fact]
    public async Task Parallel_marks_of_one_id_leave_one_row()
    {
        var store = await CreateReadyStoreAsync();
        var processedAt = DateTimeOffset.UtcNow;
        var tasks = Enumerable.Range(0, 20)
            .Select(_ => store.MarkProcessedAsync("same-id", processedAt));

        var ex = await Record.ExceptionAsync(() => Task.WhenAll(tasks));
        Assert.Null(ex);
        Assert.Equal(1, await CountRowsAsync(store));
        Assert.True(await store.IsProcessedAsync("same-id"));
    }

    [Fact]
    public async Task Reactor_drops_a_duplicate_delivery()
    {
        var store = await CreateReadyStoreAsync();
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var reactor = new CountingReactor();
        var service = new ReactorHostedService<CountingReactor, PingEvent>(
            reactor,
            transport,
            store,
            NullLogger<ReactorHostedService<CountingReactor, PingEvent>>.Instance);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await WaitForSubscriberAsync(transport);

        var context = new MessageContext { MessageId = "dup-1" };
        var evt = new PingEvent(Guid.NewGuid(), DateTime.UtcNow);
        await transport.PublishAsync(evt, context);
        await transport.PublishAsync(evt, context);

        Assert.Equal(1, reactor.CallCount);
        Assert.Equal(1, await CountRowsAsync(store));

        var storedKey = await ReadSingleKeyAsync(store);
        Assert.Equal(InboxConsumerKey.ForReactor(typeof(CountingReactor), "dup-1"), storedKey);
        Assert.EndsWith(":dup-1", storedKey, StringComparison.Ordinal);
        Assert.NotEqual("dup-1", storedKey);

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
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

    private async Task<SqlServerInboxStore> CreateReadyStoreAsync(TimeSpan? window = null)
    {
        var store = CreateStore(window: window);
        await store.InitializeSchemaAsync();
        return store;
    }

    private SqlServerInboxStore CreateStore(string? table = null, TimeSpan? window = null)
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
                InboxDeduplicationWindow = window ?? TimeSpan.FromHours(24)
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
