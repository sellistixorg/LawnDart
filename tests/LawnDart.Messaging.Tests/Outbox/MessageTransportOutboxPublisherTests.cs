using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using LawnDart;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventStore;
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
            EventType = EventTypeCatalog.TryGetDeclaredName(typeof(OrderPlacedEvent))!,
            SchemaVersion = 1,
            CodecId = EventCodec.Json,
            Payload = JsonSerializer.SerializeToUtf8Bytes(evt, evt.GetType(), JsonOptions),
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
    public async Task PublishAsync_CopiesTraceHeadersFromMetadata()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        MessageContext? receivedCtx = null;

        await transport.SubscribeAsync<OrderPlacedEvent>((_, ctx, _) =>
        {
            receivedCtx = ctx;
            return Task.CompletedTask;
        });

        var publisher = new MessageTransportOutboxPublisher(transport, [typeof(OrderPlacedEvent)]);
        var evt = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "order-trace");

        await publisher.PublishAsync(new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = EventTypeCatalog.TryGetDeclaredName(typeof(OrderPlacedEvent))!,
            SchemaVersion = 1,
            CodecId = EventCodec.Json,
            Payload = JsonSerializer.SerializeToUtf8Bytes(evt, evt.GetType(), JsonOptions),
            Metadata = JsonSerializer.Serialize(new EventMetadata
            {
                TraceId = "0af7651916cd43dd8448eb211c80319c",
                SpanId = "b7ad6b7169203331",
            }, JsonOptions),
            CreatedAt = DateTime.UtcNow,
            StreamId = "Order:trace",
            SequencePosition = 1,
        });

        Assert.NotNull(receivedCtx);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", receivedCtx.Headers["TraceId"]);
        Assert.Equal("b7ad6b7169203331", receivedCtx.Headers["SpanId"]);
        Assert.Equal(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            receivedCtx.Headers["traceparent"]);
    }

    [Fact]
    public async Task PublishAsync_UnknownEventType_Throws()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var publisher = new MessageTransportOutboxPublisher(transport, [typeof(OrderPlacedEvent)]);

        var ex = await Assert.ThrowsAsync<UnknownEventFamilyException>(() =>
            publisher.PublishAsync(new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = "Missing.Event.Type",
                Payload = "{}"u8.ToArray(),
                Metadata = "{}",
                CreatedAt = DateTime.UtcNow,
                StreamId = "s",
                SequencePosition = 1,
            }));

        Assert.Contains("Missing.Event.Type", ex.Message);
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
            EventType = EventTypeCatalog.TryGetDeclaredName(typeof(OrderPlacedEvent))!,
            SchemaVersion = 1,
            CodecId = EventCodec.Json,
            Payload = JsonSerializer.SerializeToUtf8Bytes(evt, evt.GetType(), JsonOptions),
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
    public async Task PublishAsync_UsesCatalogSchemaVersion_NotTokenOnlyMap()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        IEvent? received = null;
        await transport.SubscribeAsync<OrderPlacedV2>((e, _, _) =>
        {
            received = e;
            return Task.CompletedTask;
        });

        var catalog = EventTypeCatalog.Materialize([typeof(OrderPlacedV1), typeof(OrderPlacedV2)]);
        var publisher = new MessageTransportOutboxPublisher(transport, catalog);
        var v2 = new OrderPlacedV2(Guid.NewGuid(), DateTime.UtcNow, "order-v2", "note");

        await publisher.PublishAsync(new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "order-placed",
            SchemaVersion = 2,
            CodecId = EventCodec.Json,
            Payload = JsonSerializer.SerializeToUtf8Bytes(v2, v2.GetType(), JsonOptions),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "Order:v2",
            SequencePosition = 3,
        });

        var published = Assert.IsType<OrderPlacedV2>(received);
        Assert.Equal("order-v2", published.OrderId);
        Assert.Equal("note", published.Note);
    }

    [Fact]
    public async Task PublishAsync_V1Row_UpcastsToCurrent()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        IEvent? received = null;
        await transport.SubscribeAsync<OrderPlacedV2>((e, _, _) =>
        {
            received = e;
            return Task.CompletedTask;
        });

        var catalog = EventTypeCatalog.Materialize([typeof(OrderPlacedV1), typeof(OrderPlacedV2)]);
        var pipeline = EventUpcastPipeline.Materialize(catalog, [typeof(OrderPlacedV1ToV2)]);
        var publisher = new MessageTransportOutboxPublisher(transport, catalog, pipeline);
        var v1 = new OrderPlacedV1(Guid.NewGuid(), DateTime.UtcNow, "order-v1");

        await publisher.PublishAsync(new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "order-placed",
            SchemaVersion = 1,
            CodecId = EventCodec.Json,
            Payload = JsonSerializer.SerializeToUtf8Bytes(v1, v1.GetType(), JsonOptions),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "Order:v1",
            SequencePosition = 4,
        });

        var published = Assert.IsType<OrderPlacedV2>(received);
        Assert.Equal("order-v1", published.OrderId);
        Assert.Equal("", published.Note);
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

    [EventTypeName("order-placed", version: 1)]
    public sealed record OrderPlacedV1(Guid Id, DateTime Timestamp, string OrderId) : IEvent;

    [EventTypeName("order-placed", version: 2, current: true)]
    public sealed record OrderPlacedV2(Guid Id, DateTime Timestamp, string OrderId, string Note) : IEvent;

    public sealed class OrderPlacedV1ToV2 : IEventUpcaster<OrderPlacedV2, OrderPlacedV1>
    {
        public OrderPlacedV2 Upcast(OrderPlacedV1 source)
            => new(source.Id, source.Timestamp, source.OrderId, "");
    }
}
