using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventSourcing.SqlServer.Outbox;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;
using Testcontainers.MsSql;
using Xunit;

namespace LawnDart.EventSourcing.SqlServer.Tests.Outbox;

/// <summary>
/// Integration tests for transactional outbox pattern.
/// Verifies that events and outbox messages are written atomically.
/// </summary>
[Trait("Category", "Integration")]
public class TransactionalOutboxIntegrationTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private string? _connectionString;

    public async Task InitializeAsync()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .Build();

        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }
    }

    [Fact]
    public async Task AppendAsync_WithOutboxEnabled_WritesEventAndOutboxInSameTransaction()
    {
        // Arrange
        var outboxWriter = new SqlServerOutboxWriter(_connectionString!, "Outbox");
        await outboxWriter.InitializeSchemaAsync();

        var options = new SqlServerEventStoreOptions
        {
            EnableOutbox = true,
            OutboxTableName = "Outbox",
            RequireTenantId = false
        };

        var eventStore = new SqlServerEventStore(
            _connectionString!,
            null,
            "Events",
            "Streams",
            true,
            options,
            outboxWriter,
            null);

        await eventStore.InitializeSchemaAsync();

        var streamId = "test-tenant:TestAggregate:stream-1";
        var @event = new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "test" };
        var metadata = new EventMetadata { TenantId = null };

        // Act
        await eventStore.AppendAsync(streamId, new[] { @event }, null, metadata, new[] { "test-tag" });

        // Assert - Verify event was written
        var events = await eventStore.ReadStreamAsync(streamId);
        Assert.Single(events);
        Assert.Equal(@event.Id, events[0].Event.Id);

        // Assert - Verify outbox message was written
        var outboxMessages = await outboxWriter.GetUnprocessedAsync(10);
        Assert.Single(outboxMessages);
        Assert.Equal(streamId, outboxMessages[0].StreamId);
    }

    [Fact]
    public async Task AppendAsync_WithOutboxDisabled_DoesNotWriteToOutbox()
    {
        // Arrange
        var outboxWriter = new SqlServerOutboxWriter(_connectionString!, "Outbox");
        await outboxWriter.InitializeSchemaAsync();

        var options = new SqlServerEventStoreOptions
        {
            EnableOutbox = false, // Disabled
            OutboxTableName = "Outbox",
            RequireTenantId = false
        };

        var eventStore = new SqlServerEventStore(
            _connectionString!,
            null,
            "Events",
            "Streams",
            true,
            options,
            outboxWriter,
            null);

        await eventStore.InitializeSchemaAsync();

        var streamId = "test-tenant:TestAggregate:stream-2";
        var @event = new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "test" };
        var metadata = new EventMetadata { TenantId = null };

        // Act
        await eventStore.AppendAsync(streamId, new[] { @event }, null, metadata, new[] { "test-tag" });

        // Assert - Verify event was written
        var events = await eventStore.ReadStreamAsync(streamId);
        Assert.Single(events);

        // Assert - Verify NO outbox message was written
        var outboxMessages = await outboxWriter.GetUnprocessedAsync(10);
        Assert.Empty(outboxMessages);
    }

    [Fact]
    public async Task AppendAsync_WithTransactionRollback_RollsBackBothEventAndOutbox()
    {
        // Arrange
        var outboxWriter = new SqlServerOutboxWriter(_connectionString!, "Outbox");
        await outboxWriter.InitializeSchemaAsync();

        var options = new SqlServerEventStoreOptions
        {
            EnableOutbox = true,
            OutboxTableName = "Outbox",
            RequireTenantId = false
        };

        var eventStore = new SqlServerEventStore(
            _connectionString!,
            null,
            "Events",
            "Streams",
            true,
            options,
            outboxWriter,
            null);

        await eventStore.InitializeSchemaAsync();

        var streamId = "test-tenant:TestAggregate:stream-3";
        var event1 = new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "test1" };
        var metadata = new EventMetadata { TenantId = null };

        // Write first event
        await eventStore.AppendAsync(streamId, new[] { event1 }, null, metadata, new[] { "test-tag" });

        // Act & Assert - Try to append with wrong expected version (should rollback)
        var event2 = new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "test2" };
        
        await Assert.ThrowsAsync<ConcurrencyException>(async () =>
            await eventStore.AppendAsync(streamId, new[] { event2 }, 999, metadata, new[] { "test-tag" }));

        // Assert - Verify only first event exists
        var events = await eventStore.ReadStreamAsync(streamId);
        Assert.Single(events);
        Assert.Equal(event1.Id, events[0].Event.Id);

        // Assert - Verify only first outbox message exists
        var outboxMessages = await outboxWriter.GetUnprocessedAsync(10);
        Assert.Single(outboxMessages);
    }

    [Fact]
    public async Task AppendAsync_WithMultipleEvents_WritesAllToOutbox()
    {
        // Arrange
        var outboxWriter = new SqlServerOutboxWriter(_connectionString!, "Outbox");
        await outboxWriter.InitializeSchemaAsync();

        var options = new SqlServerEventStoreOptions
        {
            EnableOutbox = true,
            OutboxTableName = "Outbox",
            RequireTenantId = false
        };

        var eventStore = new SqlServerEventStore(
            _connectionString!,
            null,
            "Events",
            "Streams",
            true,
            options,
            outboxWriter,
            null);

        await eventStore.InitializeSchemaAsync();

        var streamId = "test-tenant:TestAggregate:stream-4";
        var events = new[]
        {
            new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "event1" },
            new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "event2" },
            new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "event3" }
        };
        var metadata = new EventMetadata { TenantId = null };

        // Act
        await eventStore.AppendAsync(streamId, events, null, metadata, new[] { "test-tag" });

        // Assert - Verify all events were written
        var storedEvents = await eventStore.ReadStreamAsync(streamId);
        Assert.Equal(3, storedEvents.Count);

        // Assert - Verify all outbox messages were written
        var outboxMessages = await outboxWriter.GetUnprocessedAsync(10);
        Assert.Equal(3, outboxMessages.Count);
        Assert.All(outboxMessages, msg => Assert.Equal(streamId, msg.StreamId));
    }

    [Fact]
    public async Task OutboxWriter_ProcessingFlow_MarksMessagesAsProcessed()
    {
        // Arrange
        var outboxWriter = new SqlServerOutboxWriter(_connectionString!, "Outbox");
        await outboxWriter.InitializeSchemaAsync();

        var options = new SqlServerEventStoreOptions
        {
            EnableOutbox = true,
            OutboxTableName = "Outbox",
            RequireTenantId = false
        };

        var eventStore = new SqlServerEventStore(
            _connectionString!,
            null,
            "Events",
            "Streams",
            true,
            options,
            outboxWriter,
            null);

        await eventStore.InitializeSchemaAsync();

        var streamId = "test-tenant:TestAggregate:stream-5";
        var @event = new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "test" };
        var metadata = new EventMetadata { TenantId = null };

        await eventStore.AppendAsync(streamId, new[] { @event }, null, metadata, new[] { "test-tag" });

        // Act - Get unprocessed messages
        var unprocessed = await outboxWriter.GetUnprocessedAsync(10);
        Assert.Single(unprocessed);

        var message = unprocessed[0];
        await outboxWriter.MarkAsProcessedAsync(message.Id);

        // Assert - No more unprocessed messages
        var stillUnprocessed = await outboxWriter.GetUnprocessedAsync(10);
        Assert.Empty(stillUnprocessed);
    }

    [Fact]
    public async Task OutboxWriter_FailureRecording_TracksFailedAttempts()
    {
        // Arrange
        var outboxWriter = new SqlServerOutboxWriter(_connectionString!, "Outbox");
        await outboxWriter.InitializeSchemaAsync();

        var options = new SqlServerEventStoreOptions
        {
            EnableOutbox = true,
            OutboxTableName = "Outbox",
            RequireTenantId = false
        };

        var eventStore = new SqlServerEventStore(
            _connectionString!,
            null,
            "Events",
            "Streams",
            true,
            options,
            outboxWriter,
            null);

        await eventStore.InitializeSchemaAsync();

        var streamId = "test-tenant:TestAggregate:stream-6";
        var @event = new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "test" };
        var metadata = new EventMetadata { TenantId = null };

        await eventStore.AppendAsync(streamId, new[] { @event }, null, metadata, new[] { "test-tag" });

        // Act - Record failures
        var unprocessed = await outboxWriter.GetUnprocessedAsync(10);
        var message = unprocessed[0];

        await outboxWriter.RecordFailureAsync(message.Id, "Test error 1");
        await outboxWriter.RecordFailureAsync(message.Id, "Test error 2");

        // Assert - Message still unprocessed but has failure attempts
        var stillUnprocessed = await outboxWriter.GetUnprocessedAsync(10);
        Assert.Single(stillUnprocessed);
        Assert.True(stillUnprocessed[0].Attempts >= 2);
    }

    // Test types
    private record TestEvent : IEvent
    {
        public Guid Id { get; init; }
        public DateTime Timestamp { get; init; }
        public string Value { get; init; } = string.Empty;
    }
}
