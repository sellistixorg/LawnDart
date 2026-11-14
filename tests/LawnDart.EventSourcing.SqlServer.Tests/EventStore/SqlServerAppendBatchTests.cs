using System.Diagnostics;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Publishing;
using LawnDart.TestUtilities;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

/// <summary>
/// A SQL Server append is one command. Commit is separate.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SqlServerAppendBatchTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private string _connectionString = "";

    public async Task InitializeAsync()
    {
        _container = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025)
                .WithPassword("Test123!")
                .Build());
        _connectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

    [Fact]
    public async Task StreamAppend_IsOneBatch_AndKeepsEachEventsTags()
    {
        var store = await OpenAsync("batchstream");
        var streamId = "s-" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(streamId, [Event("warmup")]);

        var commands = new List<string>();
        using (Capture(commands))
        {
            await store.AppendAsync(streamId, [Event("warmup", "only-a"), Event("warmup", "only-b")], expectedVersion: 1);
        }

        var batch = Assert.Single(commands);
        AssertOneAppendBatch(batch);
        Assert.True(
            batch.IndexOf("sp_getapplock", StringComparison.OrdinalIgnoreCase)
            < batch.IndexOf("THROW " + SqlServerEventStore.AppendVersionConflictError, StringComparison.Ordinal));
        Assert.True(
            batch.IndexOf("THROW " + SqlServerEventStore.AppendVersionConflictError, StringComparison.Ordinal)
            < batch.IndexOf("sp_sequence_get_range", StringComparison.OrdinalIgnoreCase));

        var stored = await ((IEventLog)store).ReadStreamAsync(streamId);
        Assert.Equal(3, stored.Count);
        Assert.Equal(["only-a"], stored[1].Tags);
        Assert.Equal(["only-b"], stored[2].Tags);
        Assert.Equal(stored[1].SequencePosition + 1, stored[2].SequencePosition);
        Assert.Equal(2, stored[1].StreamVersion);
        Assert.Equal(3, stored[2].StreamVersion);
    }

    [Fact]
    public async Task DcbAppend_IsOneBatch_WithLocksBeforeTheCondition()
    {
        var store = await OpenAsync("batchdcb");
        await store.AppendAsync("warm-" + Guid.NewGuid().ToString("N"), [Event("warmup")]);
        var tag = "course:" + Guid.NewGuid().ToString("N");

        var commands = new List<string>();
        using (Capture(commands))
        {
            await store.AppendAsync(
                [Event("warmup", tag)],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag + "-absent"))));
        }

        var batch = Assert.Single(commands);
        AssertOneAppendBatch(batch);
        Assert.Contains("sp_getapplock", batch, StringComparison.OrdinalIgnoreCase);
        Assert.True(
            batch.IndexOf("sp_getapplock", StringComparison.OrdinalIgnoreCase)
            < batch.IndexOf("THROW " + SqlServerEventStore.AppendConditionConflictError, StringComparison.Ordinal));
        Assert.True(
            batch.IndexOf("THROW " + SqlServerEventStore.AppendConditionConflictError, StringComparison.Ordinal)
            < batch.IndexOf("sp_sequence_get_range", StringComparison.OrdinalIgnoreCase));

        var conflictCommands = new List<string>();
        using (Capture(conflictCommands))
        {
            var conflict = await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
                [Event("warmup", tag)],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag)))));
            Assert.Null(conflict.ActualPosition);
        }

        Assert.Single(conflictCommands);
    }

    [Fact]
    public async Task WrongExpectedVersion_ConflictsWithoutASecondVersionRead()
    {
        var store = await OpenAsync("batchver");
        var streamId = "s-" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(streamId, [Event("first")]);

        var conflict = await Assert.ThrowsAsync<ConcurrencyException>(() =>
            store.AppendAsync(streamId, [Event("second")], expectedVersion: 0));
        Assert.Equal(0, conflict.ExpectedPosition);
        Assert.Equal(1, conflict.ActualPosition);
        Assert.Null(conflict.InnerException);

        var stored = await ((IEventLog)store).ReadStreamAsync(streamId);
        Assert.Single(stored);
    }

    [Fact]
    public async Task OutboxRows_GoInTheSameBatch()
    {
        var schema = "batchout";
        var store = await OpenAsync(schema, enableOutbox: true);
        var writer = new LawnDart.EventSourcing.SqlServer.Outbox.SqlServerOutboxWriter(
            _connectionString, schemaName: schema);
        await writer.InitializeSchemaAsync();
        await store.AppendAsync("warm-" + Guid.NewGuid().ToString("N"), [Event("warmup")]);

        var commands = new List<string>();
        using (Capture(commands))
        {
            await store.AppendAsync("out-" + Guid.NewGuid().ToString("N"), [
                Event("warmup").WithOutbound(new OutboundEvent("notice", "{}"u8.ToArray())),
                Event("warmup")
            ]);
        }

        var batch = Assert.Single(commands);
        AssertOneAppendBatch(batch);
        Assert.Contains("Outbox", batch, StringComparison.Ordinal);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM [{schema}].[Outbox]", connection);
        Assert.Equal(1, Convert.ToInt32(await command.ExecuteScalarAsync()));
    }

    private async Task<SqlServerEventStore> OpenAsync(string schema, bool enableOutbox = false)
    {
        var store = new SqlServerEventStore(
            _connectionString,
            options: new SqlServerEventStoreOptions
            {
                SchemaName = schema,
                RequireTenantId = false,
                EnableOutbox = enableOutbox
            });
        await store.InitializeSchemaAsync();
        return store;
    }

    private static AppendEvent Event(string type, params string[] tags)
        => new(type, "{}"u8.ToArray(), tags: tags);

    private static IDisposable Capture(List<string> commands)
        => new CommandCapture(commands);

    private static void AssertOneAppendBatch(string batch)
    {
        Assert.Contains("sp_getapplock", batch, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, CountOf(batch, "sp_sequence_get_range"));
        Assert.DoesNotContain("SELECT NEXT VALUE FOR", batch, StringComparison.OrdinalIgnoreCase);
        Assert.True(
            batch.IndexOf("sp_getapplock", StringComparison.OrdinalIgnoreCase)
            < batch.IndexOf("sp_sequence_get_range", StringComparison.OrdinalIgnoreCase));
    }

    private static int CountOf(string text, string value)
    {
        var count = 0;
        var index = 0;
        while ((index = text.IndexOf(value, index, StringComparison.OrdinalIgnoreCase)) >= 0)
        {
            count++;
            index += value.Length;
        }

        return count;
    }

    private sealed class CommandCapture : IObserver<DiagnosticListener>, IDisposable
    {
        private readonly List<string> _commands;
        private readonly List<IDisposable> _subscriptions = [];
        private readonly IDisposable _all;

        public CommandCapture(List<string> commands)
        {
            _commands = commands;
            _all = DiagnosticListener.AllListeners.Subscribe(this);
        }

        public void OnNext(DiagnosticListener value)
            => _subscriptions.Add(value.Subscribe(new CommandObserver(_commands)));

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        public void Dispose()
        {
            _all.Dispose();
            foreach (var subscription in _subscriptions)
                subscription.Dispose();
        }
    }

    private sealed class CommandObserver : IObserver<KeyValuePair<string, object?>>
    {
        private readonly List<string> _commands;

        public CommandObserver(List<string> commands) => _commands = commands;

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (!string.Equals(value.Key, "Microsoft.Data.SqlClient.WriteCommandBefore", StringComparison.Ordinal))
                return;

            var command = value.Value?.GetType().GetProperty("Command")?.GetValue(value.Value) as SqlCommand;
            _commands.Add(command?.CommandText ?? "");
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
