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
            Payload = "{\"value\":\"test\"}",
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
                Payload = "{}",
                Metadata = "{}",
                CreatedAt = DateTime.UtcNow,
                StreamId = "stream1",
                SequencePosition = 1
            },
            new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = "TestEvent2",
                Payload = "{}",
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
            Payload = "{}",
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };
        var message2 = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}",
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
            Payload = "{}",
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 2
        };
        var message2 = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}",
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
                Payload = "{}",
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
            Payload = "{}",
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
            Payload = "{}",
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
            Payload = "{}",
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
            Payload = "{}",
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };
        var poison = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "Poison",
            Payload = "{}",
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
    public async Task WriteAsync_PersistsSchemaVersionAndContentType()
    {
        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "family.order-placed",
            SchemaVersion = 2,
            ContentType = "application/json",
            Payload = """{"Kind":"v2"}""",
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        };

        await _writer!.WriteAsync(message);

        var read = Assert.Single(await _writer.GetUnprocessedAsync(10));
        Assert.Equal(2, read.SchemaVersion);
        Assert.Equal(AppendEvent.DefaultContentType, read.ContentType);
        Assert.Equal("family.order-placed", read.EventType);
    }

    [Fact]
    public async Task GetUnprocessedAsync_OldRowWithoutEnvelopeColumns_ReadsVersion1AndJson()
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
                ProcessedAt DATETIME2 NULL,
                Attempts INT NOT NULL DEFAULT 0,
                LastError NVARCHAR(MAX) NULL,
                LastAttemptAt DATETIME2 NULL,
                StreamId NVARCHAR(500) NOT NULL,
                SequencePosition BIGINT NOT NULL,
                DeadLetteredAt DATETIME2 NULL
            );", connection))
        {
            await create.ExecuteNonQueryAsync();
        }

        var id = Guid.NewGuid();
        await using (var insert = new SqlCommand($@"
            INSERT INTO [dbo].[{table}]
                (Id, EventType, Payload, Metadata, CreatedAt, Attempts, StreamId, SequencePosition)
            VALUES
                (@Id, @EventType, @Payload, @Metadata, @CreatedAt, 0, @StreamId, @SequencePosition);",
            connection))
        {
            insert.Parameters.AddWithValue("@Id", id);
            insert.Parameters.AddWithValue("@EventType", "legacy.event");
            insert.Parameters.AddWithValue("@Payload", "{}");
            insert.Parameters.AddWithValue("@Metadata", "{}");
            insert.Parameters.AddWithValue("@CreatedAt", DateTime.UtcNow);
            insert.Parameters.AddWithValue("@StreamId", "stream-legacy");
            insert.Parameters.AddWithValue("@SequencePosition", 1L);
            await insert.ExecuteNonQueryAsync();
        }

        var legacyWriter = new SqlServerOutboxWriter(
            _connectionString!,
            table,
            NullLogger<SqlServerOutboxWriter>.Instance);
        await legacyWriter.InitializeSchemaAsync();

        var read = Assert.Single(await legacyWriter.GetUnprocessedAsync(10));
        Assert.Equal(id, read.Id);
        Assert.Equal(1, read.SchemaVersion);
        Assert.Equal(AppendEvent.DefaultContentType, read.ContentType);
        Assert.Equal("legacy.event", read.EventType);
    }
}
