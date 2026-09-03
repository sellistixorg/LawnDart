using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;

namespace LawnDart.Messaging.Tests.InMemory;

public class InMemoryMessageTransportTests
{
    private readonly InMemoryMessageTransport _transport =
        new(NullLogger<InMemoryMessageTransport>.Instance);

    [Fact]
    public async Task PublishAsync_WithSubscriber_InvokesHandler()
    {
        var received = new List<OrderPlacedEvent>();
        var context = MessageContext.New();

        await _transport.SubscribeAsync<OrderPlacedEvent>(
            (e, ctx, ct) => { received.Add(e); return Task.CompletedTask; });

        var @event = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "order-1");
        await _transport.PublishAsync(@event, context);

        Assert.Single(received);
        Assert.Equal("order-1", received[0].OrderId);
    }

    [Fact]
    public async Task PublishAsync_WithMultipleSubscribers_InvokesAllHandlers()
    {
        int callCount = 0;
        var context = MessageContext.New();

        await _transport.SubscribeAsync<OrderPlacedEvent>(
            (_, _, _) => { callCount++; return Task.CompletedTask; });
        await _transport.SubscribeAsync<OrderPlacedEvent>(
            (_, _, _) => { callCount++; return Task.CompletedTask; });

        await _transport.PublishAsync(
            new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "order-1"), context);

        Assert.Equal(2, callCount);
    }

    [Fact]
    public async Task PublishAsync_WithNoSubscribers_DoesNotThrow()
    {
        var context = MessageContext.New();
        var @event = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "order-1");

        var ex = await Record.ExceptionAsync(() => _transport.PublishAsync(@event, context));
        Assert.Null(ex);
    }

    [Fact]
    public async Task PublishAsync_PropagatesContextToHandler()
    {
        MessageContext? received = null;
        var context = new MessageContext
        {
            MessageId = "msg-42",
            CorrelationId = "corr-1",
            TenantId = "tenant-a"
        };

        await _transport.SubscribeAsync<OrderPlacedEvent>(
            (_, ctx, _) => { received = ctx; return Task.CompletedTask; });

        await _transport.PublishAsync(
            new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "x"), context);

        Assert.NotNull(received);
        Assert.Equal("msg-42", received.MessageId);
        Assert.Equal("corr-1", received.CorrelationId);
        Assert.Equal("tenant-a", received.TenantId);
    }

    [Fact]
    public async Task SubscribeAsync_WhenCancelled_RemovesHandler()
    {
        int callCount = 0;
        using var cts = new CancellationTokenSource();

        var subscriptionTask = _transport.SubscribeAsync<OrderPlacedEvent>(
            (_, _, _) => { callCount++; return Task.CompletedTask; },
            cts.Token);

        // Publish before cancellation — handler should fire.
        await _transport.PublishAsync(
            new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "x"), MessageContext.New());
        Assert.Equal(1, callCount);

        // Cancel the subscription.
        await cts.CancelAsync();
        await subscriptionTask;

        // Publish after cancellation — handler should not fire.
        await _transport.PublishAsync(
            new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "y"), MessageContext.New());
        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task PublishAsync_DifferentMessageTypes_OnlyInvokesMatchingHandler()
    {
        var orderEvents = new List<OrderPlacedEvent>();
        var inventoryEvents = new List<InventoryReservedEvent>();

        await _transport.SubscribeAsync<OrderPlacedEvent>(
            (e, _, _) => { orderEvents.Add(e); return Task.CompletedTask; });
        await _transport.SubscribeAsync<InventoryReservedEvent>(
            (e, _, _) => { inventoryEvents.Add(e); return Task.CompletedTask; });

        await _transport.PublishAsync(
            new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "o1"), MessageContext.New());

        Assert.Single(orderEvents);
        Assert.Empty(inventoryEvents);
    }
}
