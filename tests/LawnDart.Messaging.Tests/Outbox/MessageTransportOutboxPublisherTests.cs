using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.EventSourcing.Outbox;
using LawnDart.Messaging.InMemory;
using LawnDart.Messaging.Outbox;
using LawnDart.Metadata;
using LawnDart.Outbox;

namespace LawnDart.Messaging.Tests.Outbox;

public class MessageTransportOutboxPublisherTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    [Fact]
    public async Task PublishAsync_DeserializesAndPublishesToTransport()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        OrderPlacedEvent? received = null;
        MessageContext? receivedCtx = null;

        await transport.SubscribeAsync<OrderPlacedEvent>((e, ctx, _) =>
        {
            received = e;
            receivedCtx = ctx;
            return Task.CompletedTask;
        });

        var publisher = new MessageTransportOutboxPublisher(transport, [typeof(OrderPlacedEvent)]);
        var orderId = "order-42";
        var evt = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, orderId);
        var outboxId = Guid.NewGuid();

        await publisher.PublishAsync(new OutboxMessage
        {
            Id = outboxId,
            EventType = typeof(OrderPlacedEvent).FullName!,
            Payload = JsonSerializer.Serialize(evt, evt.GetType(), JsonOptions),
            Metadata = JsonSerializer.Serialize(new EventMetadata
            {
                CorrelationId = "corr-1",
                TenantId = "tenant-a",
                UserId = "user-1",
            }, JsonOptions),
            CreatedAt = DateTime.UtcNow,
            StreamId = "Order:1",
            SequencePosition = 7,
        });

        Assert.NotNull(received);
        Assert.Equal(orderId, received.OrderId);
        Assert.NotNull(receivedCtx);
        Assert.Equal(outboxId.ToString(), receivedCtx.MessageId);
        Assert.Equal("corr-1", receivedCtx.CorrelationId);
        Assert.Equal("tenant-a", receivedCtx.TenantId);
        Assert.Equal("Order:1", receivedCtx.Headers["outbox.streamId"]);
        Assert.Equal("7", receivedCtx.Headers["outbox.sequencePosition"]);
    }

    [Fact]
    public async Task PublishAsync_UnknownEventType_Throws()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var publisher = new MessageTransportOutboxPublisher(transport, [typeof(OrderPlacedEvent)]);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            publisher.PublishAsync(new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = "Missing.Event.Type",
                Payload = "{}",
                Metadata = "{}",
                CreatedAt = DateTime.UtcNow,
                StreamId = "s",
                SequencePosition = 1,
            }));

        Assert.Contains("No CLR type registered", ex.Message);
    }

    [Fact]
    public async Task OutboxProcessor_InvokesPublisher_AndMarksProcessed()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var received = new List<OrderPlacedEvent>();
        await transport.SubscribeAsync<OrderPlacedEvent>((e, _, _) =>
        {
            received.Add(e);
            return Task.CompletedTask;
        });

        var publisher = new MessageTransportOutboxPublisher(transport, [typeof(OrderPlacedEvent)]);
        var writer = new InMemoryOutboxWriter();
        var evt = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "order-99");

        await writer.WriteAsync(new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = typeof(OrderPlacedEvent).FullName!,
            Payload = JsonSerializer.Serialize(evt, evt.GetType(), JsonOptions),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "Order:99",
            SequencePosition = 1,
        });

        var processor = new TestableOutboxProcessor(writer, publisher);
        await processor.ProcessBatchPublicAsync(CancellationToken.None);

        Assert.Single(received);
        Assert.Equal("order-99", received[0].OrderId);
        Assert.Empty(await writer.GetUnprocessedAsync(10));
    }

    [Fact]
    public void AddMessageTransportOutboxPublisher_ResolvesFromDi()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageTransport>(
            new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance));
        services.AddMessageTransportOutboxPublisher(typeof(OrderPlacedEvent));

        using var sp = services.BuildServiceProvider();
        var publisher = sp.GetRequiredService<IOutboxPublisher>();
        Assert.IsType<MessageTransportOutboxPublisher>(publisher);
    }

    /// <summary>Minimal in-memory outbox store for processor smoke tests.</summary>
    private sealed class InMemoryOutboxWriter : IOutboxWriter
    {
        private readonly List<OutboxMessage> _messages = new();

        public Task WriteAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            _messages.Add(message);
            return Task.CompletedTask;
        }

        public Task WriteBatchAsync(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken = default)
        {
            _messages.AddRange(messages);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<OutboxMessage>> GetUnprocessedAsync(int batchSize, CancellationToken cancellationToken = default)
        {
            var unprocessed = _messages
                .Where(m => m.ProcessedAt is null && m.DeadLetteredAt is null)
                .OrderBy(m => m.SequencePosition)
                .Take(batchSize)
                .ToList();
            return Task.FromResult<IReadOnlyList<OutboxMessage>>(unprocessed);
        }

        public Task<IReadOnlyList<OutboxMessage>> GetDeadLetteredAsync(int batchSize, CancellationToken cancellationToken = default)
        {
            var dead = _messages
                .Where(m => m.DeadLetteredAt is not null)
                .OrderBy(m => m.SequencePosition)
                .Take(batchSize)
                .ToList();
            return Task.FromResult<IReadOnlyList<OutboxMessage>>(dead);
        }

        public Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
        {
            var msg = _messages.Single(m => m.Id == messageId);
            msg.ProcessedAt = DateTime.UtcNow;
            return Task.CompletedTask;
        }

        public Task MarkAsDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
        {
            var msg = _messages.Single(m => m.Id == messageId);
            msg.DeadLetteredAt ??= DateTime.UtcNow;
            return Task.CompletedTask;
        }

        public Task RecordFailureAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
        {
            var msg = _messages.Single(m => m.Id == messageId);
            msg.Attempts++;
            msg.LastError = error;
            msg.LastAttemptAt = DateTime.UtcNow;
            return Task.CompletedTask;
        }

        public Task InitializeSchemaAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    /// <summary>Exposes <see cref="OutboxProcessor"/> batch processing for tests.</summary>
    private sealed class TestableOutboxProcessor : OutboxProcessor
    {
        public TestableOutboxProcessor(IOutboxWriter writer, IOutboxPublisher publisher)
            : base(writer, publisher, NullLogger<OutboxProcessor>.Instance, maxAttempts: 10)
        {
        }

        public Task ProcessBatchPublicAsync(CancellationToken cancellationToken)
        {
            var method = typeof(OutboxProcessor).GetMethod(
                "ProcessBatchAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            return (Task)method!.Invoke(this, [cancellationToken])!;
        }
    }
}
