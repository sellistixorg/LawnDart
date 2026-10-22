using System.Text;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.Backends.Contract.Tests;

public abstract class OutboxContractTests : ContractSuite
{
    protected OutboxContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [ContractFact]
    public async Task WriteAsync_AddsMessageToOutbox()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var message = NewMessage("stream1", 1);
        await writer.WriteAsync(message);
        var unprocessed = await writer.GetUnprocessedAsync(10);
        Assert.Equal(message.Id, Assert.Single(unprocessed).Id);
    }

    [ContractFact]
    public async Task WriteBatchAsync_AddsMultipleMessages()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var messages = new[] { NewMessage("stream1", 1), NewMessage("stream1", 2) };
        await writer.WriteBatchAsync(messages);
        Assert.Equal(2, (await writer.GetUnprocessedAsync(10)).Count);
    }

    [ContractFact]
    public async Task GetUnprocessedAsync_ReturnsOnlyUnprocessedMessages()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var keep = NewMessage("s", 1);
        var done = NewMessage("s", 2);
        await writer.WriteAsync(keep);
        await writer.WriteAsync(done);
        await writer.MarkAsProcessedAsync(done.Id);
        Assert.Equal(keep.Id, Assert.Single(await writer.GetUnprocessedAsync(10)).Id);
    }

    [ContractFact]
    public async Task GetUnprocessedAsync_RespectsOrderBySequencePosition()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        await writer.WriteAsync(NewMessage("s", 3));
        await writer.WriteAsync(NewMessage("s", 1));
        await writer.WriteAsync(NewMessage("s", 2));
        var rows = await writer.GetUnprocessedAsync(10);
        Assert.Equal(3, rows.Count);
        Assert.Equal(1, rows[0].SequencePosition);
        Assert.Equal(2, rows[1].SequencePosition);
        Assert.Equal(3, rows[2].SequencePosition);
    }

    [ContractFact]
    public async Task GetUnprocessedAsync_RespectsBatchSize()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        for (var i = 1; i <= 5; i++)
            await writer.WriteAsync(NewMessage("s", i));
        Assert.Equal(3, (await writer.GetUnprocessedAsync(3)).Count);
    }

    [ContractFact]
    public async Task GetUnprocessedAsync_HonorsClaimLease()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var held = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.FromHours(1)));
        var heldMessage = NewMessage("held", 1);
        await held.WriteAsync(heldMessage);
        Assert.Equal(heldMessage.Id, Assert.Single(await held.GetUnprocessedAsync(10)).Id);
        Assert.Empty(await held.GetUnprocessedAsync(10));

        var open = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        var openMessage = NewMessage("open", 1);
        await open.WriteAsync(openMessage);
        Assert.Equal(openMessage.Id, Assert.Single(await open.GetUnprocessedAsync(10)).Id);
        Assert.Equal(openMessage.Id, Assert.Single(await open.GetUnprocessedAsync(10)).Id);
    }

    [ContractFact]
    public async Task MarkAsProcessedAsync_SetsProcessedAt()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var message = NewMessage("s", 1);
        await writer.WriteAsync(message);
        await writer.MarkAsProcessedAsync(message.Id);
        Assert.Empty(await writer.GetUnprocessedAsync(10));
    }

    [ContractFact]
    public async Task RecordFailureAsync_IncrementsAttemptsAndSetsError()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        var message = NewMessage("s", 1);
        await writer.WriteAsync(message);
        await writer.RecordFailureAsync(message.Id, "boom");
        var row = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(1, row.Attempts);
        Assert.Equal("boom", row.LastError);
        Assert.NotNull(row.LastAttemptAt);
    }

    [ContractFact]
    public async Task RecordFailureAsync_MultipleFailures_IncrementsAttempts()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        var message = NewMessage("s", 1);
        await writer.WriteAsync(message);
        await writer.RecordFailureAsync(message.Id, "one");
        await writer.RecordFailureAsync(message.Id, "two");
        await writer.RecordFailureAsync(message.Id, "three");
        var row = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(3, row.Attempts);
        Assert.Equal("three", row.LastError);
    }

    [ContractFact]
    public async Task MarkAsDeadLetteredAsync_ExcludesFromUnprocessed_AndIsQueryable()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var live = NewMessage("s", 1);
        var poison = NewMessage("s", 2);
        await writer.WriteAsync(live);
        await writer.WriteAsync(poison);
        await writer.MarkAsDeadLetteredAsync(poison.Id);
        var unprocessed = await writer.GetUnprocessedAsync(10);
        Assert.Equal(live.Id, Assert.Single(unprocessed).Id);
        var dead = Assert.Single(await writer.GetDeadLetteredAsync(10));
        Assert.Equal(poison.Id, dead.Id);
        Assert.NotNull(dead.DeadLetteredAt);
        Assert.Null(dead.ProcessedAt);
    }

    [ContractFact]
    public async Task WriteAsync_PersistsSchemaVersionAndCodecId()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var message = NewMessage("s", 1, schemaVersion: 2, codecId: 2);
        await writer.WriteAsync(message);
        var row = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(2, row.SchemaVersion);
        Assert.Equal(2, row.CodecId);
        Assert.Equal(message.EventType, row.EventType);
    }

    [ContractFact]
    public async Task ResetDeadLettered_ClearsDeadLetter_KeepsLastError_AndZeroesAttempts()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        var message = NewMessage("s", 1, attempts: 4);
        await writer.WriteAsync(message);
        await writer.RecordFailureAsync(message.Id, "broker down");
        await writer.MarkAsDeadLetteredAsync(message.Id);
        var before = Assert.Single(await writer.GetDeadLetteredAsync(10));
        Assert.True(await writer.ResetDeadLetteredAsync(message.Id));
        var row = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(message.Id, row.Id);
        Assert.Equal(0, row.Attempts);
        Assert.Null(row.DeadLetteredAt);
        Assert.Null(row.ProcessedAt);
        Assert.Equal("broker down", row.LastError);
        Assert.Equal(before.LastAttemptAt, row.LastAttemptAt);
        Assert.Empty(await writer.GetDeadLetteredAsync(10));
    }

    [ContractFact]
    public async Task ResetDeadLettered_Unknown_NeverDeadLettered_AndProcessed_ReturnFalse()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        Assert.False(await writer.ResetDeadLetteredAsync(Guid.NewGuid()));
        var live = NewMessage("s", 1, attempts: 2);
        await writer.WriteAsync(live);
        await writer.RecordFailureAsync(live.Id, "once");
        Assert.False(await writer.ResetDeadLetteredAsync(live.Id));
        var stillLive = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(3, stillLive.Attempts);
        Assert.Equal("once", stillLive.LastError);
        var processed = NewMessage("s", 2, attempts: 6);
        await writer.WriteAsync(processed);
        await writer.MarkAsDeadLetteredAsync(processed.Id);
        await writer.MarkAsProcessedAsync(processed.Id);
        Assert.False(await writer.ResetDeadLetteredAsync(processed.Id));
        Assert.DoesNotContain(await writer.GetUnprocessedAsync(10), m => m.Id == processed.Id);
        var stillDead = (await writer.GetDeadLetteredAsync(10)).Single(m => m.Id == processed.Id);
        Assert.Equal(6, stillDead.Attempts);
        Assert.NotNull(stillDead.ProcessedAt);
        Assert.NotNull(stillDead.DeadLetteredAt);
        Assert.False(await writer.ResetDeadLetteredAsync(live.Id));
    }

    [ContractFact]
    public async Task ResetDeadLettered_SecondCall_ReturnsFalse()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var message = NewMessage("s", 1);
        await writer.WriteAsync(message);
        await writer.MarkAsDeadLetteredAsync(message.Id);
        Assert.True(await writer.ResetDeadLetteredAsync(message.Id));
        Assert.False(await writer.ResetDeadLetteredAsync(message.Id));
        var queued = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(0, queued.Attempts);
    }

    [ContractFact]
    public async Task ResetAll_ReturnsCount_AndSkipsLiveAndProcessedRows()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        var live = NewMessage("s", 1);
        await writer.WriteAsync(live);
        await writer.RecordFailureAsync(live.Id, "live");
        await writer.RecordFailureAsync(live.Id, "live-2");
        var processed = NewMessage("s", 2);
        await writer.WriteAsync(processed);
        await writer.MarkAsProcessedAsync(processed.Id);
        for (var i = 0; i < 3; i++)
        {
            var dead = NewMessage("s", 10 + i);
            await writer.WriteAsync(dead);
            await writer.MarkAsDeadLetteredAsync(dead.Id);
        }

        Assert.Equal(3, await writer.ResetAllDeadLetteredAsync());
        Assert.Equal(0, await writer.ResetAllDeadLetteredAsync());
        var queued = await writer.GetUnprocessedAsync(10);
        Assert.Equal(4, queued.Count);
        Assert.All(queued.Where(m => m.Id != live.Id), m => Assert.Equal(0, m.Attempts));
        Assert.Equal(2, queued.Single(m => m.Id == live.Id).Attempts);
        Assert.DoesNotContain(queued, m => m.Id == processed.Id);
        Assert.Empty(await writer.GetDeadLetteredAsync(10));
    }

    [ContractFact]
    public async Task Reset_ThenOutboxProcessor_PublishesAndMarksProcessed()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var writer = await Backend.OpenOutboxAsync(ContractStores.Isolated(outboxClaimLease: TimeSpan.Zero));
        var publisher = new RecordingPublisher { ShouldFail = true };
        var message = NewMessage("s", 1);
        await writer.WriteAsync(message);
        var processor = new BatchProcessor(writer, publisher, maxAttempts: 3);
        for (var i = 0; i < 6; i++)
        {
            await processor.ProcessBatchAsync();
            if ((await writer.GetDeadLetteredAsync(10)).Any(m => m.Id == message.Id))
                break;
        }

        Assert.Contains(await writer.GetDeadLetteredAsync(10), m => m.Id == message.Id);
        Assert.True(await writer.ResetDeadLetteredAsync(message.Id));
        publisher.ShouldFail = false;
        await processor.ProcessBatchAsync();
        Assert.Contains(publisher.Published, m => m.Id == message.Id);
        Assert.DoesNotContain(await writer.GetUnprocessedAsync(10), m => m.Id == message.Id);
        Assert.DoesNotContain(await writer.GetDeadLetteredAsync(10), m => m.Id == message.Id);
    }

    [ContractFact]
    public async Task Write_ToOneContext_NotVisibleFromAnother()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var ordering = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var catalog = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var message = NewMessage("order", 1);
        await ordering.WriteAsync(message);
        Assert.Empty(await catalog.GetUnprocessedAsync(10));
        Assert.Equal(message.Id, Assert.Single(await ordering.GetUnprocessedAsync(10)).Id);
    }

    [ContractFact]
    public async Task MarkAsProcessed_OnlyAffectsOwnContext()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var ordering = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var catalog = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var left = NewMessage("order", 1);
        var right = NewMessage("product", 1);
        await ordering.WriteAsync(left);
        await catalog.WriteAsync(right);
        await ordering.MarkAsProcessedAsync(left.Id);
        Assert.Empty(await ordering.GetUnprocessedAsync(10));
        Assert.Equal(right.Id, Assert.Single(await catalog.GetUnprocessedAsync(10)).Id);
    }

    [ContractFact]
    public async Task ResetDeadLettered_DoesNotTouchAnotherContext()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        var ordering = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var catalog = await Backend.OpenOutboxAsync(ContractStores.Isolated());
        var left = NewMessage("order", 1, attempts: 4);
        var right = NewMessage("product", 1, attempts: 4);
        await ordering.WriteAsync(left);
        await catalog.WriteAsync(right);
        await ordering.MarkAsDeadLetteredAsync(left.Id);
        await catalog.MarkAsDeadLetteredAsync(right.Id);
        Assert.True(await ordering.ResetDeadLetteredAsync(left.Id));
        var requeued = Assert.Single(await ordering.GetUnprocessedAsync(10));
        Assert.Equal(left.Id, requeued.Id);
        Assert.Equal(0, requeued.Attempts);
        Assert.Null(requeued.DeadLetteredAt);
        var otherDead = Assert.Single(await catalog.GetDeadLetteredAsync(10));
        Assert.Equal(right.Id, otherDead.Id);
        Assert.NotNull(otherDead.DeadLetteredAt);
        Assert.Empty(await catalog.GetUnprocessedAsync(10));
        await ordering.MarkAsDeadLetteredAsync(left.Id);
        var extra = NewMessage("order", 2, attempts: 4);
        await ordering.WriteAsync(extra);
        await ordering.MarkAsDeadLetteredAsync(extra.Id);
        Assert.Equal(2, await ordering.ResetAllDeadLetteredAsync());
        Assert.Empty(await ordering.GetDeadLetteredAsync(10));
        Assert.Equal(right.Id, Assert.Single(await catalog.GetDeadLetteredAsync(10)).Id);
    }

    [ContractFact]
    public async Task AppendAsync_WithOutboxEnabled_WritesEventAndOutboxInSameTransaction()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(enableOutbox: true));
        var streamId = "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");
        var @event = new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "test" };
        await opened.Store.AppendAsync(streamId, [@event], metadata: new EventMetadata(), tags: ["test-tag"]);
        Assert.Equal(@event.Id, Assert.Single(await opened.Store.ReadStreamAsync(streamId)).Event.Id);
        Assert.Equal(streamId, Assert.Single(await opened.Outbox.GetUnprocessedAsync(10)).StreamId);
    }

    [ContractFact]
    public async Task AppendAsync_WithOutboxEnabled_CopiesFrameBytesNotClrReserialize()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(enableOutbox: true));
        var streamId = "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");
        var @event = new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "frame-copy" };
        await opened.Store.AppendAsync(
            streamId,
            [@event],
            metadata: new EventMetadata { UserId = "outbox-user" },
            tags: ["test-tag"]);
        var recorded = Assert.Single(await opened.Log.ReadStreamAsync(streamId));
        var outbox = Assert.Single(await opened.Outbox.GetUnprocessedAsync(10));
        Assert.Equal(recorded.EventType, outbox.EventType);
        Assert.Equal(recorded.SchemaVersion, outbox.SchemaVersion);
        Assert.Equal(recorded.CodecId, outbox.CodecId);
        AssertStoredPayload(recorded.Payload.ToArray(), outbox.Payload, recorded.CodecId);
        AssertStoredMetadata(recorded.Metadata.ToArray(), Encoding.UTF8.GetBytes(outbox.Metadata));
        Assert.Equal("backend-contract.outbox-event", outbox.EventType);
        Assert.Contains("frame-copy", Encoding.UTF8.GetString(outbox.Payload.Span), StringComparison.Ordinal);
        Assert.Contains("outbox-user", outbox.Metadata, StringComparison.Ordinal);
    }

    [ContractFact]
    public async Task AppendAsync_WithOutboxDisabled_DoesNotWriteToOutbox()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(includeOutboxReader: true));
        var streamId = "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");
        await opened.Store.AppendAsync(streamId, [new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "off" }]);
        Assert.Single(await opened.Store.ReadStreamAsync(streamId));
        Assert.Empty(await opened.Outbox.GetUnprocessedAsync(10));
    }

    [ContractFact]
    public async Task AppendAsync_WithTransactionRollback_RollsBackBothEventAndOutbox()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(enableOutbox: true));
        var streamId = "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");
        var event1 = new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "one" };
        await opened.Store.AppendAsync(streamId, [event1]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => opened.Store.AppendAsync(
            streamId,
            [new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "two" }],
            expectedVersion: 5));
        var stored = Assert.Single(await opened.Store.ReadStreamAsync(streamId));
        Assert.Equal(event1.Id, stored.Event.Id);
        Assert.Single(await opened.Outbox.GetUnprocessedAsync(10));
    }

    [ContractFact]
    public async Task AppendAsync_WithMultipleEvents_WritesAllToOutbox()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(enableOutbox: true));
        var streamId = "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");
        await opened.Store.AppendAsync(streamId, [
            new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "a" },
            new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "b" },
            new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "c" }
        ]);
        Assert.Equal(3, (await opened.Store.ReadStreamAsync(streamId)).Count);
        var outboxMessages = await opened.Outbox.GetUnprocessedAsync(10);
        Assert.Equal(3, outboxMessages.Count);
        Assert.All(outboxMessages, message => Assert.Equal(streamId, message.StreamId));
    }

    [ContractFact]
    public async Task OutboxWriter_ProcessingFlow_MarksMessagesAsProcessed()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(enableOutbox: true, outboxClaimLease: TimeSpan.Zero));
        var streamId = "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");
        await opened.Store.AppendAsync(streamId, [new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "p" }]);
        var message = Assert.Single(await opened.Outbox.GetUnprocessedAsync(10));
        await opened.Outbox.MarkAsProcessedAsync(message.Id);
        Assert.Empty(await opened.Outbox.GetUnprocessedAsync(10));
    }

    [ContractFact]
    public async Task OutboxWriter_FailureRecording_TracksFailedAttempts()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(enableOutbox: true, outboxClaimLease: TimeSpan.Zero));
        await opened.Store.AppendAsync(Stream(), [new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "f" }]);
        var message = Assert.Single(await opened.Outbox.GetUnprocessedAsync(10));
        await opened.Outbox.RecordFailureAsync(message.Id, "nope");
        var again = Assert.Single(await opened.Outbox.GetUnprocessedAsync(10));
        Assert.Equal(1, again.Attempts);
        Assert.Equal("nope", again.LastError);
    }

    [ContractFact]
    public async Task AppendAsync_EnableOutbox_WithoutWriter_StillWritesRows()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(enableOutbox: true));
        var streamId = Stream();
        await opened.Store.AppendAsync(streamId, [new ContractOutboxEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, Value = "solo" }]);
        var recorded = Assert.Single(await opened.Log.ReadStreamAsync(streamId));
        var reader = await Backend.OpenOutboxAsync(new ContractStoreOptions
        {
            SchemaName = opened.SchemaName,
            RequireTenantId = false,
            OutboxClaimLease = TimeSpan.Zero
        });
        var outbox = Assert.Single(await reader.GetUnprocessedAsync(10));
        Assert.Equal(recorded.CodecId, outbox.CodecId);
        AssertStoredPayload(recorded.Payload.ToArray(), outbox.Payload, recorded.CodecId);
    }

    [ContractFact]
    public async Task AppendAsync_LogEnvelope_CopiesSchemaVersionAndCodecIdToOutbox()
    {
        SkipUnless(Cap.HasOutboxStore, "This backend does not have an outbox yet.");
        SkipUnless(Cap.AppendWritesOutboxRows, "This backend does not write outbox rows when an event is appended.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(enableOutbox: true));
        var streamId = Stream();
        var payload = Encoding.UTF8.GetBytes("""{"Kind":"v2"}""");
        await opened.Log.AppendAsync(streamId, [new AppendEvent(
            "contract.outbox.envelope",
            payload,
            schemaVersion: 2,
            codecId: EventCodec.Json,
            tags: ["env"])]);
        var row = Assert.Single(await opened.Outbox.GetUnprocessedAsync(10));
        Assert.Equal("contract.outbox.envelope", row.EventType);
        Assert.Equal(2, row.SchemaVersion);
        Assert.Equal(EventCodec.Json, row.CodecId);
        Assert.Equal(payload, row.Payload.ToArray());
    }

    private static string Stream() => "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");

    private static OutboxMessage NewMessage(string stream, long sequence, int schemaVersion = 1, byte codecId = 1, int attempts = 0)
        => new()
        {
            Id = Guid.NewGuid(),
            EventType = "contract.outbox",
            SchemaVersion = schemaVersion,
            CodecId = codecId,
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            Attempts = attempts,
            StreamId = stream,
            SequencePosition = sequence
        };

    private sealed class RecordingPublisher : IOutboxPublisher
    {
        public bool ShouldFail { get; set; }
        public List<OutboxMessage> Published { get; } = [];

        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            if (ShouldFail)
                throw new InvalidOperationException("Simulated publish failure");
            Published.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class BatchProcessor : OutboxProcessor
    {
        public BatchProcessor(IOutboxWriter writer, IOutboxPublisher publisher, int maxAttempts)
            : base(writer, publisher, NullLogger<OutboxProcessor>.Instance, TimeSpan.FromSeconds(5), batchSize: 100, maxAttempts)
        {
        }

        public Task ProcessBatchAsync()
        {
            var method = typeof(OutboxProcessor).GetMethod(
                "ProcessBatchAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return (Task)method!.Invoke(this, [CancellationToken.None])!;
        }
    }
}
