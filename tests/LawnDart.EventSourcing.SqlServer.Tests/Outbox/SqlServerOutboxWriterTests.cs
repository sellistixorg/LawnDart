using Xunit;
using LawnDart.EventStore;
using LawnDart.Outbox;
using LawnDart.EventSourcing.SqlServer.Outbox;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.Outbox;

[Trait("Category", "Integration")]
public class SqlServerOutboxWriterTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private SqlServerOutboxWriter? _writer;
    private string? _connectionString;

    public async Task InitializeAsync()
    {
        _container = new MsSqlBuilder(MsSqlTestImage.Server2022)
            .Build();

        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
        
        _writer = new SqlServerOutboxWriter(_connectionString, "Outbox", NullLogger<SqlServerOutboxWriter>.Instance);
        await _writer.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }
    }

    [Fact]
    public async Task WriteAsync_AddsMessageToOutbox()
    {
        // Arrange
        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{\"value\":\"test\"}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };

        // Act
        await _writer!.WriteAsync(message);

        // Assert
        var unprocessed = await _writer.GetUnprocessedAsync(10);
        Assert.Single(unprocessed);
        Assert.Equal(message.Id, unprocessed[0].Id);
    }

    [Fact]
    public async Task WriteBatchAsync_AddsMultipleMessages()
    {
        // Arrange
        var messages = new[]
        {
            new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = "TestEvent1",
                Payload = "{}"u8.ToArray(),
                Metadata = "{}",
                CreatedAt = DateTime.UtcNow,
                StreamId = "stream1",
                SequencePosition = 1
            },
            new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = "TestEvent2",
                Payload = "{}"u8.ToArray(),
                Metadata = "{}",
                CreatedAt = DateTime.UtcNow,
                StreamId = "stream1",
                SequencePosition = 2
            }
        };

        // Act
        await _writer!.WriteBatchAsync(messages);

        // Assert
        var unprocessed = await _writer.GetUnprocessedAsync(10);
        Assert.Equal(2, unprocessed.Count);
    }

    [Fact]
    public async Task GetUnprocessedAsync_ReturnsOnlyUnprocessedMessages()
    {
        // Arrange
        var message1 = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };
        var message2 = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 2
        };

        await _writer!.WriteAsync(message1);
        await _writer.WriteAsync(message2);
        await _writer.MarkAsProcessedAsync(message1.Id);

        // Act
        var unprocessed = await _writer.GetUnprocessedAsync(10);

        // Assert
        Assert.Single(unprocessed);
        Assert.Equal(message2.Id, unprocessed[0].Id);
    }

    [Fact]
    public async Task GetUnprocessedAsync_RespectsOrderBySequencePosition()
    {
        // Arrange
        var message1 = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 2
        };
        var message2 = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };

        // Write in reverse order
        await _writer!.WriteAsync(message1);
        await _writer.WriteAsync(message2);

        // Act
        var unprocessed = await _writer.GetUnprocessedAsync(10);

        // Assert - Should be ordered by sequence position
        Assert.Equal(2, unprocessed.Count);
        Assert.Equal(1, unprocessed[0].SequencePosition);
        Assert.Equal(2, unprocessed[1].SequencePosition);
    }

    [Fact]
    public async Task GetUnprocessedAsync_RespectsBatchSize()
    {
        // Arrange
        for (int i = 0; i < 5; i++)
        {
            await _writer!.WriteAsync(new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = "TestEvent",
                Payload = "{}"u8.ToArray(),
                Metadata = "{}",
                CreatedAt = DateTime.UtcNow,
                StreamId = "stream1",
                SequencePosition = i
            });
        }

        // Act
        var unprocessed = await _writer!.GetUnprocessedAsync(3);

        // Assert
        Assert.Equal(3, unprocessed.Count);
    }

    [Fact]
    public async Task MarkAsProcessedAsync_SetsProcessedAt()
    {
        // Arrange
        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };
        await _writer!.WriteAsync(message);

        // Act
        await _writer.MarkAsProcessedAsync(message.Id);

        // Assert
        var unprocessed = await _writer.GetUnprocessedAsync(10);
        Assert.Empty(unprocessed);
    }

    [Fact]
    public async Task RecordFailureAsync_IncrementsAttemptsAndSetsError()
    {
        // Arrange
        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };
        await _writer!.WriteAsync(message);

        // Act
        await _writer.RecordFailureAsync(message.Id, "Test error");

        // Assert
        var unprocessed = await _writer.GetUnprocessedAsync(10);
        Assert.Single(unprocessed);
        Assert.Equal(1, unprocessed[0].Attempts);
        Assert.Equal("Test error", unprocessed[0].LastError);
        Assert.NotNull(unprocessed[0].LastAttemptAt);
    }

    [Fact]
    public async Task RecordFailureAsync_MultipleFailures_IncrementsAttempts()
    {
        // Arrange
        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };
        await _writer!.WriteAsync(message);

        // Act
        await _writer.RecordFailureAsync(message.Id, "Error 1");
        await _writer.RecordFailureAsync(message.Id, "Error 2");
        await _writer.RecordFailureAsync(message.Id, "Error 3");

        // Assert
        var unprocessed = await _writer.GetUnprocessedAsync(10);
        Assert.Single(unprocessed);
        Assert.Equal(3, unprocessed[0].Attempts);
        Assert.Equal("Error 3", unprocessed[0].LastError);
    }

    [Fact]
    public async Task MarkAsDeadLetteredAsync_ExcludesFromUnprocessed_AndIsQueryable()
    {
        var live = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "Live",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };
        var poison = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "Poison",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 2
        };
        await _writer!.WriteAsync(live);
        await _writer.WriteAsync(poison);

        await _writer.MarkAsDeadLetteredAsync(poison.Id);

        var unprocessed = await _writer.GetUnprocessedAsync(10);
        Assert.Single(unprocessed);
        Assert.Equal(live.Id, unprocessed[0].Id);

        var dead = await _writer.GetDeadLetteredAsync(10);
        Assert.Single(dead);
        Assert.Equal(poison.Id, dead[0].Id);
        Assert.NotNull(dead[0].DeadLetteredAt);
        Assert.Null(dead[0].ProcessedAt);
    }

    [Fact]
    public async Task WriteAsync_PersistsSchemaVersionAndCodecId()
    {
        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "family.order-placed",
            SchemaVersion = 2,
            CodecId = EventCodec.Json,
            Payload = """{"Kind":"v2"}"""u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };

        await _writer!.WriteAsync(message);

        var read = Assert.Single(await _writer.GetUnprocessedAsync(10));
        Assert.Equal(2, read.SchemaVersion);
        Assert.Equal(EventCodec.Json, read.CodecId);
        Assert.Equal("family.order-placed", read.EventType);
    }

    [Fact]
    public async Task Outbox_table_layout_is_varbinary_codec_id_no_defaults()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        await using (var cmd = new SqlCommand(
            """
            SELECT c.name, t.name AS type_name
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(N'[dbo].[Outbox]')
            """,
            connection))
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                columns[reader.GetString(0)] = reader.GetString(1);
        }

        Assert.False(columns.ContainsKey("ContentType"));
        Assert.Equal("varbinary", columns["Payload"]);
        Assert.Equal("tinyint", columns["CodecId"]);
        Assert.Equal("int", columns["SchemaVersion"]);

        await using (var cmd = new SqlCommand(
            """
            SELECT c.name
            FROM sys.default_constraints dc
            JOIN sys.columns c
                ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
            WHERE dc.parent_object_id = OBJECT_ID(N'[dbo].[Outbox]')
              AND c.name IN (N'SchemaVersion', N'CodecId')
            """,
            connection))
        {
            var defaults = new List<string>();
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                defaults.Add(reader.GetString(0));
            Assert.Empty(defaults);
        }
    }

    [Fact]
    public async Task Pre_stamp_outbox_table_throws_wipe_exception()
    {
        const string table = "OutboxLegacy";
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using (var create = new SqlCommand($@"
            CREATE TABLE [dbo].[{table}] (
                Id UNIQUEIDENTIFIER PRIMARY KEY,
                EventType NVARCHAR(500) NOT NULL,
                Payload NVARCHAR(MAX) NOT NULL,
                Metadata NVARCHAR(MAX) NOT NULL,
                CreatedAt DATETIME2 NOT NULL,
                StreamId NVARCHAR(500) NOT NULL,
                SequencePosition BIGINT NOT NULL
            );", connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        var legacyWriter = new SqlServerOutboxWriter(
            _connectionString!,
            table,
            NullLogger<SqlServerOutboxWriter>.Instance);

        var ex = await Assert.ThrowsAsync<IncompatibleOutboxSchemaException>(
            () => legacyWriter.InitializeSchemaAsync());
        Assert.Null(ex.FoundFormat);
        Assert.Equal(SqlServerOutboxWriter.CurrentSchemaFormat, ex.RequiredFormat);
        Assert.Contains("Drop and recreate", ex.Message, StringComparison.Ordinal);
    }
}
