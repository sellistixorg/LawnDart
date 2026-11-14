using LawnDart;
using LawnDart.Messaging;
using LawnDart.Messaging.Tests;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.EventProcessing;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.Messaging.Tests.Hosting;

public class EventProcessorStartupTests
{
    [Fact]
    public async Task StartAsync_subscribes_before_it_returns()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var inbox = new InMemoryInboxStore(Microsoft.Extensions.Options.Options.Create(new MessagingOptions()));
        var service = new EventProcessorHostedService<QuietProcessor, OrderPlacedEvent>(
            new QuietProcessor(),
            transport,
            inbox,
            NullLogger<EventProcessorHostedService<QuietProcessor, OrderPlacedEvent>>.Instance);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        Assert.Equal(1, transport.SubscriberCount<OrderPlacedEvent>());

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
        Assert.Equal(0, transport.SubscriberCount<OrderPlacedEvent>());
    }

    private sealed class QuietProcessor : IEventProcessor<OrderPlacedEvent>
    {
        public Task<IEnumerable<IEvent>> ProcessAsync(
            OrderPlacedEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<IEvent>>([]);
    }
}
