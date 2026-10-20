using System.Text;
using System.Text.RegularExpressions;
using LawnDart;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.TestUtilities;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

/// <summary>
/// Regressions for skipped events, a stale event-type cache, and tag case.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SqlServerEventStoreCorrectnessTests : IAsyncLifetime
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
    public async Task RolledBackSequence_DoesNotStallTheNextCommit()
    {
        var logger = new ListLogger();
        var store = CreateStore(_connectionString, "gaproll", TimeSpan.FromMilliseconds(200), logger);
        await store.InitializeSchemaAsync();
        await store.AppendAsync("s1", [Evt()]);

        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
            await using (var fence = new SqlCommand(
                "INSERT INTO [gaproll].[AppendFence] OUTPUT INSERTED.[Id] DEFAULT VALUES",
                connection,
                transaction))
            {
                await fence.ExecuteScalarAsync();
            }

            await using (var next = new SqlCommand(
                "SELECT NEXT VALUE FOR [gaproll].[EventSequencePosition]",
                connection,
                transaction))
            {
                await next.ExecuteScalarAsync();
            }

            await transaction.RollbackAsync();
        }

        var fast = await store.AppendAsync("s2", [Evt()]);
        Assert.Equal(fast.SequencePositions[0], await store.GetCurrentSequenceAsync());
        Assert.DoesNotContain(
            logger.Entries,
            e => e.Level == LogLevel.Warning && e.Message.Contains("Read horizon advanced", StringComparison.Ordinal));
        await using var handle = store.Subscribe("gap-sub", fromSequence: 1);
        var received = await ReadCountAsync(handle, 2, TimeSpan.FromSeconds(15));
        Assert.Equal(1, received[0].SequencePosition);
        Assert.Equal(fast.SequencePositions[0], received[1].SequencePosition);
    }



    [Fact]
    public async Task Format2_UpgradesInPlace_AndKeepsRows()
    {
        const string schema = "upg";
        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await using var command = new SqlCommand(Format2Sql, connection);
            await command.ExecuteNonQueryAsync();
        }

        var store = CreateStore(_connectionString, schema);
        await store.InitializeSchemaAsync();

        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            Assert.Equal(SqlServerEventStore.CurrentSchemaFormat, await ScalarAsync(connection, """
                SELECT CAST(value AS nvarchar(20))
                FROM sys.extended_properties
                WHERE major_id = OBJECT_ID(N'[upg].[Events]')
                  AND name = N'LawnDart_SchemaFormat'
                  AND minor_id = 0
                """));
            Assert.Equal(1, Convert.ToInt32(await ScalarAsync(connection, """
                SELECT COUNT(*) FROM sys.columns
                WHERE object_id = OBJECT_ID(N'[upg].[Events]') AND name = N'RowVersion'
                """)));
            Assert.Equal(SqlServerEventStore.TagCollation, (string)(await ScalarAsync(connection, """
                SELECT collation_name FROM sys.columns
                WHERE object_id = OBJECT_ID(N'[upg].[EventTags]') AND name = N'Tag'
                """))!);
            Assert.NotNull(await ScalarAsync(connection,
                "SELECT OBJECT_ID(N'[upg].[AppendFence]', 'U')"));
        }

        var log = (IEventLog)store;
        var exact = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags("Customer:ABC")));
        Assert.Contains(exact.Events, e => e.StreamId == "kept");
        var other = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags("customer:abc")));
        Assert.DoesNotContain(other.Events, e => e.StreamId == "kept");

        var appended = await store.AppendAsync("s2", [Evt()]);
        Assert.True(appended.SequencePositions[0] > 1);
    }

    private SqlServerEventStore CreateStore(
        string connectionString,
        string schema,
        TimeSpan? readHorizonGracePeriod = null,
        ILogger<SqlServerEventStore>? logger = null)
    {
        var options = new SqlServerEventStoreOptions
        {
            RequireTenantId = false,
            SchemaName = schema,
            ContextName = schema,
            SqlSubscriptionPollInterval = TimeSpan.FromMilliseconds(50)
        };
        if (readHorizonGracePeriod is { } grace)
            options.ReadHorizonGracePeriod = grace;

        return new SqlServerEventStore(
            connectionString,
            serializer: null,
            tableName: "Events",
            registryTableName: "Streams",
            enableRegistry: true,
            options: options,
            outboxWriter: null,
            logger: logger);
    }

    private static async Task<long> WaitForHeadAsync(SqlServerEventStore store, long expected, TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        long head = 0;
        while (DateTime.UtcNow - started < timeout)
        {
            head = await store.GetCurrentSequenceAsync();
            if (head == expected)
                return head;
            await Task.Delay(40);
        }

        return head;
    }

    private static async Task<string> WaitForLogAsync(
        ListLogger logger,
        SqlServerEventStore store,
        Func<(LogLevel Level, string Message), bool> match,
        TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        while (DateTime.UtcNow - started < timeout)
        {
            await store.GetCurrentSequenceAsync();
            var found = logger.Entries.FirstOrDefault(match);
            if (found.Message is not null)
                return found.Message;
            await Task.Delay(40);
        }

        return "";
    }

    private async Task<string> CreateDatabaseAsync(string database, bool readCommittedSnapshot)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        var flag = readCommittedSnapshot ? "ON" : "OFF";
        await using var command = new SqlCommand(
            $"CREATE DATABASE [{database}]; ALTER DATABASE [{database}] SET READ_COMMITTED_SNAPSHOT {flag} WITH ROLLBACK IMMEDIATE;",
            connection);
        await command.ExecuteNonQueryAsync();
        var builder = new SqlConnectionStringBuilder(_connectionString) { InitialCatalog = database };
        return builder.ConnectionString;
    }

    private static async Task<object?> ScalarAsync(SqlConnection connection, string sql)
    {
        await using var command = new SqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync();
        return result is DBNull ? null : result;
    }

    private static async Task<List<RecordedEvent>> ReadRecordedAsync(
        IEventLogSubscriptionHandle handle,
        int count,
        TimeSpan timeout)
    {
        var list = new List<RecordedEvent>(count);
        using var cts = new CancellationTokenSource(timeout);
        while (list.Count < count && await handle.Events.WaitToReadAsync(cts.Token))
        {
            while (list.Count < count && handle.Events.TryRead(out var recorded))
                list.Add(recorded);
        }

        if (list.Count < count)
            throw new TimeoutException($"Expected {count} events, received {list.Count}.");
        return list;
    }

    private static async Task<List<SequencedEvent>> ReadCountAsync(
        ISubscriptionHandle handle,
        int count,
        TimeSpan timeout)
    {
        var list = new List<SequencedEvent>(count);
        using var cts = new CancellationTokenSource(timeout);
        while (list.Count < count && await handle.Events.WaitToReadAsync(cts.Token))
        {
            while (list.Count < count && handle.Events.TryRead(out var sequenced))
                list.Add(sequenced);
        }

        if (list.Count < count)
            throw new TimeoutException($"Expected {count} events, received {list.Count}.");
        return list;
    }

    private static async Task<List<SequencedEvent>> DrainAsync(ISubscriptionHandle handle, TimeSpan wait)
    {
        var list = new List<SequencedEvent>();
        using var cts = new CancellationTokenSource(wait);
        try
        {
            while (await handle.Events.WaitToReadAsync(cts.Token))
            {
                while (handle.Events.TryRead(out var sequenced))
                    list.Add(sequenced);
            }
        }
        catch (OperationCanceledException)
        {
            // The wait ended.
        }

        return list;
    }

    private static StubEvent Evt() => new(Guid.NewGuid(), DateTime.UtcNow);

    private sealed class ListLogger : ILogger<SqlServerEventStore>
    {
        private readonly object _gate = new();
        private readonly List<(LogLevel Level, string Message)> _entries = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Entries
        {
            get
            {
                lock (_gate)
                    return _entries.ToArray();
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_gate)
                _entries.Add((logLevel, formatter(state, exception)));
        }
    }

    [EventTypeName("sql-correctness.stub")]
    private sealed record StubEvent(Guid Id, DateTime Timestamp) : IEvent;

    private const string Format2Sql = """
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'upg')
            EXEC('CREATE SCHEMA [upg] AUTHORIZATION dbo');
        IF OBJECT_ID(N'[upg].[EventTypes]', 'U') IS NULL
            CREATE TABLE [upg].[EventTypes] (
                [Id] INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
                [Token] NVARCHAR(500) NOT NULL UNIQUE
            );
        IF OBJECT_ID(N'[upg].[Events]', 'U') IS NULL
            CREATE TABLE [upg].[Events] (
                [StreamId] NVARCHAR(255) NOT NULL,
                [Version] BIGINT NOT NULL,
                [SequencePosition] BIGINT NOT NULL,
                [EventTypeId] INT NOT NULL,
                [EventData] VARBINARY(MAX) NOT NULL,
                [SchemaVersion] INT NOT NULL,
                [CodecId] TINYINT NOT NULL,
                [Tags] NVARCHAR(4000) NULL,
                [Metadata] NVARCHAR(MAX) NULL,
                [Timestamp] DATETIME2 NOT NULL,
                [PartitionHash] INT NOT NULL,
                PRIMARY KEY ([StreamId], [Version]),
                CONSTRAINT [FK_upg_Events_EventTypes] FOREIGN KEY ([EventTypeId]) REFERENCES [upg].[EventTypes]([Id])
            );
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_SequencePosition' AND object_id = OBJECT_ID(N'[upg].[Events]'))
            CREATE UNIQUE NONCLUSTERED INDEX [UX_SequencePosition] ON [upg].[Events]([SequencePosition]);
        IF NOT EXISTS (
            SELECT 1 FROM sys.extended_properties
            WHERE major_id = OBJECT_ID(N'[upg].[Events]') AND name = N'LawnDart_SchemaFormat' AND minor_id = 0)
            EXEC sys.sp_addextendedproperty
                @name = N'LawnDart_SchemaFormat', @value = N'2',
                @level0type = N'SCHEMA', @level0name = N'upg',
                @level1type = N'TABLE', @level1name = N'Events';
        IF NOT EXISTS (SELECT 1 FROM [upg].[EventTypes])
            INSERT INTO [upg].[EventTypes] ([Token]) VALUES (N'upgrade.keep');
        IF NOT EXISTS (SELECT 1 FROM [upg].[Events])
            INSERT INTO [upg].[Events] (
                StreamId, Version, SequencePosition, EventTypeId, EventData, SchemaVersion, CodecId,
                Tags, Metadata, Timestamp, PartitionHash)
            VALUES (
                N'kept', 1, 1, 1, 0x7B7D, 1, 1,
                N'["Customer:ABC"]', NULL, SYSUTCDATETIME(), 0);
        IF OBJECT_ID(N'[upg].[EventTags]', 'U') IS NULL
            CREATE TABLE [upg].[EventTags] (
                [GlobalSequencePosition] BIGINT NOT NULL,
                [Tag] NVARCHAR(256) NOT NULL,
                CONSTRAINT [PK_EventTags] PRIMARY KEY CLUSTERED ([Tag], [GlobalSequencePosition]),
                CONSTRAINT [FK_upg_EventTags_Events] FOREIGN KEY ([GlobalSequencePosition])
                    REFERENCES [upg].[Events]([SequencePosition])
            );
        IF NOT EXISTS (SELECT 1 FROM [upg].[EventTags])
            INSERT INTO [upg].[EventTags] (GlobalSequencePosition, Tag) VALUES (1, N'Customer:ABC');
        IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE name = N'EventSequencePosition' AND schema_id = SCHEMA_ID(N'upg'))
            CREATE SEQUENCE [upg].[EventSequencePosition] START WITH 2 INCREMENT BY 1 CACHE 1000;
        """;
}
