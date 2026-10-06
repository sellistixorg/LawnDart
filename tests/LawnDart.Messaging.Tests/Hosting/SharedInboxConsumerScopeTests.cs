using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.EventProcessing;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Messaging.Tests.Hosting;

/// <summary>
/// Two consumers of one message share a single <see cref="IInboxStore"/>.
/// Dedup must be per consumer. A shared message id would drop the second handler.
/// </summary>
public class SharedInboxConsumerScopeTests
{
    [Fact]
    public async Task Two_reactors_each_handle_one_shared_message_once()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var inbox = new InMemoryInboxStore(Options.Create(new MessagingOptions()));
        var first = new FirstOrderReactor();
        var second = new SecondOrderReactor();

        using var cts = new CancellationTokenSource();
        var firstService = new ReactorHostedService<FirstOrderReactor, OrderPlacedEvent>(
            first,
            transport,
            inbox,
            NullLogger<ReactorHostedService<FirstOrderReactor, OrderPlacedEvent>>.Instance);
        var secondService = new ReactorHostedService<SecondOrderReactor, OrderPlacedEvent>(
            second,
            transport,
            inbox,
            NullLogger<ReactorHostedService<SecondOrderReactor, OrderPlacedEvent>>.Instance);

        await firstService.StartAsync(cts.Token);
        await secondService.StartAsync(cts.Token);
        await WaitForSubscribersAsync(transport, 2);

        var context = new MessageContext { MessageId = "shared-order-1" };
        var evt = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "order-1");

        await transport.PublishAsync(evt, context);
        await transport.PublishAsync(evt, context);

        Assert.Equal(1, first.CallCount);
        Assert.Equal(1, second.CallCount);
        Assert.Equal(2, inbox.Count);

        await cts.CancelAsync();
        await firstService.StopAsync(CancellationToken.None);
        await secondService.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Reactor_and_processor_each_handle_one_shared_message_once()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var inbox = new InMemoryInboxStore(Options.Create(new MessagingOptions()));
        var reactor = new FirstOrderReactor();
        var processor = new QuietOrderProcessor();

        using var cts = new CancellationTokenSource();
        var reactorService = new ReactorHostedService<FirstOrderReactor, OrderPlacedEvent>(
            reactor,
            transport,
            inbox,
            NullLogger<ReactorHostedService<FirstOrderReactor, OrderPlacedEvent>>.Instance);
        var processorService = new EventProcessorHostedService<QuietOrderProcessor, OrderPlacedEvent>(
            processor,
            transport,
            inbox,
            NullLogger<EventProcessorHostedService<QuietOrderProcessor, OrderPlacedEvent>>.Instance);

        await reactorService.StartAsync(cts.Token);
        await processorService.StartAsync(cts.Token);
        await WaitForSubscribersAsync(transport, 2);

        var context = new MessageContext { MessageId = "shared-order-2" };
        var evt = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "order-2");

        await transport.PublishAsync(evt, context);
        await transport.PublishAsync(evt, context);

        Assert.Equal(1, reactor.CallCount);
        Assert.Equal(1, processor.CallCount);
        Assert.Equal(2, inbox.Count);

        await cts.CancelAsync();
        await reactorService.StopAsync(CancellationToken.None);
        await processorService.StopAsync(CancellationToken.None);
    }

    private static async Task WaitForSubscribersAsync(InMemoryMessageTransport transport, int count)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (transport.SubscriberCount<OrderPlacedEvent>() < count)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Expected {count} subscribers.");
            await Task.Delay(10);
        }
    }

    private sealed class FirstOrderReactor : IReactor<OrderPlacedEvent>
    {
        public int CallCount { get; private set; }

        public Task<IEnumerable<ICommand>> ReactAsync(
            OrderPlacedEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            return Task.FromResult<IEnumerable<ICommand>>([]);
        }
    }

    private sealed class SecondOrderReactor : IReactor<OrderPlacedEvent>
    {
        public int CallCount { get; private set; }

        public Task<IEnumerable<ICommand>> ReactAsync(
            OrderPlacedEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            return Task.FromResult<IEnumerable<ICommand>>([]);
        }
    }

    private sealed class QuietOrderProcessor : IEventProcessor<OrderPlacedEvent>
    {
        public int CallCount { get; private set; }

        public Task<IEnumerable<IEvent>> ProcessAsync(
            OrderPlacedEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            return Task.FromResult<IEnumerable<IEvent>>([]);
        }
    }
}
