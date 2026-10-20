using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Testcontainers.MsSql;
using Xunit;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

[Trait("Category", "Integration")]
public class SqlServerEventStoreTests : IAsyncLifetime
{
    private MsSqlContainer? _sqlContainer;
    private SqlServerEventStore? _eventStore;
    private string? _connectionString;
    private readonly string _tableName = $"Events_{Guid.NewGuid():N}";

    public async Task InitializeAsync()
    {
        _sqlContainer = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025)
                .WithPassword("Test123!")
                .Build());
        _connectionString = _sqlContainer.GetConnectionString();
        
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));
        var logger = loggerFactory.CreateLogger<SqlServerEventStore>();
        
        _eventStore = new SqlServerEventStore(_connectionString, null, _tableName, "Streams", true, new SqlServerEventStoreOptions { RequireTenantId = false }, null, logger);
        await _eventStore.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        if (_sqlContainer is not null)
            await _sqlContainer.DisposeAsync();
    }



























    [Fact]
    public async Task GetCurrentSequenceAsync_ReturnsMaxCommittedSequence_NotSequenceAllocatorHead()
    {
        var streamId = "test-tenant:TestAggregate:seq-head";
        await _eventStore!.AppendAsync(streamId, new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
        });

        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await using var burn = new SqlCommand(
                """
                DECLARE @i INT = 0;
                WHILE @i < 1000
                BEGIN
                    SELECT NEXT VALUE FOR [dbo].[EventSequencePosition];
                    SET @i = @i + 1;
                END
                """,
                connection);
            await burn.ExecuteNonQueryAsync();
        }

        var append = await _eventStore.AppendAsync(streamId, new IEvent[]
        {
            new TestEvent(Guid.NewGuid(), DateTime.UtcNow),
        });

        var lastCommitted = append.SequencePositions[^1];
        Assert.True(lastCommitted > 100, "Expected CACHE jump to produce a sequence gap before the final append");

        var head = await _eventStore.GetCurrentSequenceAsync();
        Assert.Equal(lastCommitted, head);

        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await using var seqCmd = new SqlCommand(
                """
                SELECT CAST(current_value AS BIGINT)
                FROM sys.sequences
                WHERE name = 'EventSequencePosition' AND schema_id = SCHEMA_ID('dbo')
                """,
                connection);
            var allocatorHead = Convert.ToInt64(await seqCmd.ExecuteScalarAsync());
            Assert.True(allocatorHead >= lastCommitted);
        }
    }







    [Fact]
    public async Task New_append_writes_payload_and_codec_id()
    {
        var streamId = "test-tenant:TestAggregate:binary-column";
        await _eventStore!.AppendAsync(streamId, [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            $"""
            SELECT EventData, SchemaVersion, CodecId
            FROM [dbo].[{_tableName}]
            WHERE StreamId = @StreamId
            """,
            connection);
        command.Parameters.AddWithValue("@StreamId", streamId);

        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.False(reader.IsDBNull("EventData"));
        Assert.IsType<byte[]>(reader.GetValue("EventData"));
        Assert.Equal(1, reader.GetInt32("SchemaVersion"));
        Assert.Equal(EventCodec.Json, reader.GetByte("CodecId"));

        var stored = Assert.Single(await _eventStore.ReadStreamAsync(streamId));
        Assert.NotNull(stored.Metadata.CommitTimestamp);
        Assert.Equal(1, stored.Metadata.SchemaVersion);
    }

    [Fact]
    public async Task Events_table_layout_is_varbinary_codec_id_no_defaults()
    {
        var qTable = $"[dbo].[{_tableName}]";
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var columns = new Dictionary<string, (string Type, short MaxLength)>(StringComparer.OrdinalIgnoreCase);
        await using (var cmd = new SqlCommand(
            """
            SELECT c.name, t.name AS type_name, c.max_length
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(@Table)
            """,
            connection))
        {
            cmd.Parameters.AddWithValue("@Table", qTable);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                columns[reader.GetString(0)] = (reader.GetString(1), reader.GetInt16(2));
        }

        Assert.False(columns.ContainsKey("EventPayload"));
        Assert.False(columns.ContainsKey("ContentType"));
        Assert.False(columns.ContainsKey("EventType"));
        Assert.Equal("varbinary", columns["EventData"].Type);
        Assert.Equal("tinyint", columns["CodecId"].Type);
        Assert.Equal("int", columns["SchemaVersion"].Type);
        Assert.Equal("int", columns["EventTypeId"].Type);

        await using (var cmd = new SqlCommand(
            """
            SELECT c.name
            FROM sys.default_constraints dc
            JOIN sys.columns c
                ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
            WHERE dc.parent_object_id = OBJECT_ID(@Table)
              AND c.name IN (N'SchemaVersion', N'CodecId', N'EventTypeId')
            """,
            connection))
        {
            cmd.Parameters.AddWithValue("@Table", qTable);
            var defaults = new List<string>();
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                defaults.Add(reader.GetString(0));
            Assert.Empty(defaults);
        }

        await using (var cmd = new SqlCommand(
            """
            SELECT CAST(value AS nvarchar(128))
            FROM sys.extended_properties
            WHERE major_id = OBJECT_ID(@Table)
              AND name = @Name
              AND minor_id = 0
            """,
            connection))
        {
            cmd.Parameters.AddWithValue("@Table", qTable);
            cmd.Parameters.AddWithValue("@Name", SqlServerEventStore.SchemaFormatPropertyName);
            Assert.Equal(SqlServerEventStore.CurrentSchemaFormat, (string)(await cmd.ExecuteScalarAsync())!);
        }

        Assert.True(await ObjectExistsAsync(connection, $"[dbo].[{_tableName}_Readable]", "V"));
        Assert.True(await ObjectExistsAsync(connection, $"[dbo].[{_eventStore!.EventTypesTableName}]", "U"));
    }

    [Fact]
    public async Task Partition_covering_index_includes_codec_id_and_no_max_column()
    {
        var qTable = $"[dbo].[{_tableName}]";
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var cmd = new SqlCommand(
            """
            SELECT c.name, c.max_length
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID(@Table)
              AND i.name = @IndexName
              AND ic.is_included_column = 1
            """,
            connection);
        cmd.Parameters.AddWithValue("@Table", qTable);
        cmd.Parameters.AddWithValue("@IndexName", SqlServerEventStore.PartitionHashIndexName);

        var included = new Dictionary<string, short>(StringComparer.OrdinalIgnoreCase);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            included[reader.GetString(0)] = reader.GetInt16(1);

        Assert.Contains("CodecId", included.Keys);
        Assert.Contains("SchemaVersion", included.Keys);
        Assert.Contains("EventTypeId", included.Keys);
        Assert.DoesNotContain("EventType", included.Keys);
        Assert.DoesNotContain("EventData", included.Keys);
        Assert.DoesNotContain("Tags", included.Keys);
        Assert.DoesNotContain("Metadata", included.Keys);
        Assert.All(included.Values, maxLength => Assert.NotEqual((short)-1, maxLength));
    }

    [Fact]
    public async Task Write_path_does_not_parameterize_a_mime_string()
    {
        var streamId = "test-tenant:TestAggregate:no-mime";
        await _eventStore!.AppendAsync(streamId, [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using (var cmd = new SqlCommand(
            $"""
            SELECT EventTypeId, Tags, Metadata, CodecId
            FROM [dbo].[{_tableName}]
            WHERE StreamId = @StreamId
            """,
            connection))
        {
            cmd.Parameters.AddWithValue("@StreamId", streamId);
            await using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(EventCodec.Json, reader.GetByte("CodecId"));
            Assert.True(reader.GetInt32("EventTypeId") > 0);
            Assert.DoesNotContain(EventCodec.JsonMime, reader.GetString("Tags"));
            var metadata = reader.IsDBNull("Metadata") ? string.Empty : reader.GetString("Metadata");
            Assert.DoesNotContain(EventCodec.JsonMime, metadata);
            Assert.DoesNotContain(EventCodec.MemoryPackMime, metadata);
        }

        await using (var cmd = new SqlCommand(
            """
            SELECT COUNT(*)
            FROM sys.columns
            WHERE object_id = OBJECT_ID(@Table) AND name = N'ContentType'
            """,
            connection))
        {
            cmd.Parameters.AddWithValue("@Table", $"[dbo].[{_tableName}]");
            Assert.Equal(0, (int)(await cmd.ExecuteScalarAsync())!);
        }
    }

    [Fact]
    public async Task Pre_stamp_table_throws_wipe_exception_and_is_not_altered()
    {
        var tableName = $"Events_PreStamp_{Guid.NewGuid():N}";
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using (var create = new SqlCommand(
            $"""
            CREATE TABLE [dbo].[{tableName}] (
                StreamId NVARCHAR(255) NOT NULL,
                Version BIGINT NOT NULL,
                PRIMARY KEY (StreamId, Version)
            )
            """,
            connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        var store = new SqlServerEventStore(
            _connectionString!,
            tableName: tableName,
            registryTableName: $"Streams_{tableName}",
            options: new SqlServerEventStoreOptions { RequireTenantId = false });

        var ex = await Assert.ThrowsAsync<IncompatibleEventStoreSchemaException>(
            () => store.InitializeSchemaAsync());
        Assert.Null(ex.FoundFormat);
        Assert.Equal(SqlServerEventStore.CurrentSchemaFormat, ex.RequiredFormat);
        Assert.Contains("Drop and recreate", ex.Message, StringComparison.Ordinal);

        await using var cmd = new SqlCommand(
            """
            SELECT COUNT(*)
            FROM sys.columns
            WHERE object_id = OBJECT_ID(@Table) AND name = N'CodecId'
            """,
            connection);
        cmd.Parameters.AddWithValue("@Table", $"[dbo].[{tableName}]");
        Assert.Equal(0, (int)(await cmd.ExecuteScalarAsync())!);
        Assert.False(await ObjectExistsAsync(connection, $"[dbo].[{store.EventTypesTableName}]", "U"));
    }

    [Fact]
    public async Task Format_1_stamped_table_throws_wipe_exception_and_is_not_altered()
    {
        var tableName = $"Events_Fmt1_{Guid.NewGuid():N}";
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using (var create = new SqlCommand(
            $"""
            CREATE TABLE [dbo].[{tableName}] (
                StreamId NVARCHAR(255) NOT NULL,
                Version BIGINT NOT NULL,
                EventType NVARCHAR(500) NOT NULL,
                PRIMARY KEY (StreamId, Version)
            );
            EXEC sys.sp_addextendedproperty
                @name = N'{SqlServerEventStore.SchemaFormatPropertyName}',
                @value = N'1',
                @level0type = N'SCHEMA', @level0name = N'dbo',
                @level1type = N'TABLE',  @level1name = N'{tableName}';
            """,
            connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        var store = new SqlServerEventStore(
            _connectionString!,
            tableName: tableName,
            registryTableName: $"Streams_{tableName}",
            options: new SqlServerEventStoreOptions { RequireTenantId = false });

        var ex = await Assert.ThrowsAsync<IncompatibleEventStoreSchemaException>(
            () => store.InitializeSchemaAsync());
        Assert.Equal("1", ex.FoundFormat);
        Assert.Equal(SqlServerEventStore.CurrentSchemaFormat, ex.RequiredFormat);

        await using var cmd = new SqlCommand(
            """
            SELECT COUNT(*)
            FROM sys.columns
            WHERE object_id = OBJECT_ID(@Table) AND name = N'EventTypeId'
            """,
            connection);
        cmd.Parameters.AddWithValue("@Table", $"[dbo].[{tableName}]");
        Assert.Equal(0, (int)(await cmd.ExecuteScalarAsync())!);
        Assert.False(await ObjectExistsAsync(connection, $"[dbo].[{store.EventTypesTableName}]", "U"));
    }

    [Fact]
    public async Task Concurrent_foreign_token_appends_produce_one_event_types_row()
    {
        var token = $"foreign-concurrent.{Guid.NewGuid():N}";
        var streamA = $"test-tenant:Foreign:{Guid.NewGuid():N}";
        var streamB = $"test-tenant:Foreign:{Guid.NewGuid():N}";
        var log = (IEventLog)_eventStore!;

        await Task.WhenAll(
            log.AppendAsync(streamA, [new AppendEvent(token, new byte[] { 1 })]),
            log.AppendAsync(streamB, [new AppendEvent(token, new byte[] { 2 })]));

        Assert.Equal(token, Assert.Single(await log.ReadStreamAsync(streamA)).EventType);
        Assert.Equal(token, Assert.Single(await log.ReadStreamAsync(streamB)).EventType);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var cmd = new SqlCommand(
            $"""
            SELECT COUNT(*) FROM [dbo].[{_eventStore!.EventTypesTableName}] WHERE Token = @Token
            """,
            connection);
        cmd.Parameters.AddWithValue("@Token", token);
        Assert.Equal(1, (int)(await cmd.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task Schema_init_warms_event_types_from_the_scoped_catalog()
    {
        var tableName = $"Events_Warm_{Guid.NewGuid():N}";
        var catalog = EventTypeCatalog.Materialize([typeof(TestEvent), typeof(AnotherEvent)]);
        var store = new SqlServerEventStore(
            _connectionString!,
            tableName: tableName,
            registryTableName: $"Streams_{tableName}",
            options: new SqlServerEventStoreOptions { RequireTenantId = false },
            session: new EventSession(new JsonEventSerializer(), catalog));
        await store.InitializeSchemaAsync();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var cmd = new SqlCommand(
            $"""
            SELECT Token FROM [dbo].[{store.EventTypesTableName}] ORDER BY Token
            """,
            connection);
        var tokens = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            tokens.Add(reader.GetString(0));

        Assert.Contains("sql-server-event-store-tests.test-event", tokens);
        Assert.Contains("sql-server-event-store-tests.another-event", tokens);
    }

    [Fact]
    public async Task Unknown_token_query_returns_empty_without_sql()
    {
        var built = DcbQuerySql.Build(
            "[dbo].[Events]",
            "[dbo].[EventTags]",
            Query.FromItems(QueryItem.ByType("no-such-family")),
            _ => null,
            fromSequencePosition: null,
            toSequencePosition: null,
            toTimestamp: null,
            limit: null,
            DcbQuerySql.Mode.Events);

        Assert.True(built.ShortCircuited);
        Assert.Equal("", built.Sql);

        var result = await ((IEventLog)_eventStore!).ReadByQueryAsync(
            Query.FromItems(QueryItem.ByType($"unknown-family.{Guid.NewGuid():N}")));
        Assert.Empty(result.Events);
    }

    [Fact]
    public async Task Append_round_trips_schema_version_one_and_two()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(TestEvent), typeof(SchemaV1), typeof(SchemaCurrent)]);
        var store = new SqlServerEventStore(
            _connectionString!,
            tableName: _tableName,
            options: new SqlServerEventStoreOptions { RequireTenantId = false },
            session: new EventSession(new JsonEventSerializer(), catalog));
        await store.InitializeSchemaAsync();

        var streamV1 = "test-tenant:Schema:v1";
        var streamV2 = "test-tenant:Schema:v2";
        await store.AppendAsync(streamV1, [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await store.AppendAsync(streamV2, [new SchemaCurrent(Guid.NewGuid(), DateTime.UtcNow, "Ada", "bio")]);

        var typedV1 = Assert.Single(await store.ReadStreamAsync(streamV1));
        Assert.IsType<TestEvent>(typedV1.Event);
        Assert.Equal(1, typedV1.Metadata.SchemaVersion);
        Assert.Equal(1, Assert.Single(await ((IEventLog)store).ReadStreamAsync(streamV1)).SchemaVersion);

        var typedV2 = Assert.Single(await store.ReadStreamAsync(streamV2));
        Assert.IsType<SchemaCurrent>(typedV2.Event);
        Assert.Equal(2, typedV2.Metadata.SchemaVersion);
        Assert.Equal(2, Assert.Single(await ((IEventLog)store).ReadStreamAsync(streamV2)).SchemaVersion);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            $"""
            SELECT SchemaVersion FROM [dbo].[{_tableName}] WHERE StreamId = @StreamId
            """,
            connection);
        command.Parameters.AddWithValue("@StreamId", streamV2);
        Assert.Equal(2, (int)(await command.ExecuteScalarAsync())!);
    }

    private static async Task<bool> ObjectExistsAsync(SqlConnection connection, string qualifiedName, string type)
    {
        await using var cmd = new SqlCommand(
            "SELECT CASE WHEN OBJECT_ID(@Name, @Type) IS NULL THEN 0 ELSE 1 END",
            connection);
        cmd.Parameters.AddWithValue("@Name", qualifiedName);
        cmd.Parameters.AddWithValue("@Type", type);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 1;
    }

    [EventTypeName("sql-server-event-store-tests.test-event")]
    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
    [EventTypeName("sql-server-event-store-tests.another-event")]
    private record AnotherEvent(Guid Id, DateTime Timestamp) : IEvent;
    [EventTypeName("sql-server-event-store-tests.event-with-properties")]
    private record EventWithProperties(Guid Id, DateTime Timestamp, string ProductId, int Quantity) : IEvent;
    [EventTypeName("tests.sql.alias-event")]
    private record SqlAliasedEvent(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("sql-server-event-store-tests.schema-evolved", version: 1)]
    private record SchemaV1(Guid Id, DateTime Timestamp, string Name) : IEvent;

    [EventTypeName("sql-server-event-store-tests.schema-evolved", version: 2, current: true)]
    private record SchemaCurrent(Guid Id, DateTime Timestamp, string Name, string Bio) : IEvent;
}

