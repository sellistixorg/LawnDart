using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.Messaging;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Messaging.Tests.Hosting;

public class ReactorHostedServiceTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Handle_InboundCorrelation_IsKeptOnChildDispatchContext()
    {
        using var listener = EnableAllActivities();
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var inbox = new InMemoryInboxStore(Options.Create(new MessagingOptions()));
        var dispatcher = new CapturingCommandDispatcher();
        var reactor = new OrderPlacedReactor();
        var service = new ReactorHostedService<OrderPlacedReactor, OrderPlacedEvent>(
            reactor,
            transport,
            inbox,
            NullLogger<ReactorHostedService<OrderPlacedReactor, OrderPlacedEvent>>.Instance,
            dispatcher);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await WaitForSubscriberAsync(transport);

        var inbound = new MessageContext
        {
            MessageId = "reactor-msg-1",
            CorrelationId = "saga-corr-9",
            Headers = new Dictionary<string, string>
            {
                ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
            }
        };

        await transport.PublishAsync(
            new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "order-traced"),
            inbound);

        await dispatcher.WaitForDispatchAsync(1, Timeout);

        var child = Assert.Single(dispatcher.Contexts);
        Assert.Equal("saga-corr-9", child.CorrelationId);
        Assert.Equal("reactor-msg-1", child.CausationId);
        Assert.Contains("0af7651916cd43dd8448eb211c80319c", child.Headers["traceparent"]);

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Handle_WhenClaimIsHeld_ThrowsAndDoesNotRunTheReactor()
    {
        var transport = new InMemoryMessageTransport(
            NullLogger<InMemoryMessageTransport>.Instance,
            Options.Create(new MessageTransportOptions { MaxDeliveryAttempts = 1 }));
        var inbox = new InMemoryInboxStore(Options.Create(new MessagingOptions
        {
            InboxClaimLease = TimeSpan.FromHours(1)
        }));
        var reactor = new CountingReactor();
        var service = new ReactorHostedService<CountingReactor, OrderPlacedEvent>(
            reactor,
            transport,
            inbox,
            NullLogger<ReactorHostedService<CountingReactor, OrderPlacedEvent>>.Instance);
        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await WaitForCountingSubscriberAsync(transport);

        var messageId = "held-1";
        Assert.True(await inbox.TryClaimAsync(InboxConsumerKey.ForReactor(typeof(CountingReactor), messageId)));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(
            new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "held"),
            new MessageContext { MessageId = messageId }));
        Assert.Contains("claim", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, reactor.CallCount);

        await inbox.ReleaseClaimAsync(InboxConsumerKey.ForReactor(typeof(CountingReactor), messageId));
        await transport.PublishAsync(
            new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "later"),
            new MessageContext { MessageId = messageId });
        Assert.Equal(1, reactor.CallCount);

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Handle_WhenAlreadyProcessed_SkipsAndDoesNotRunTheReactorAgain()
    {
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var inbox = new InMemoryInboxStore(Options.Create(new MessagingOptions()));
        var reactor = new CountingReactor();
        var service = new ReactorHostedService<CountingReactor, OrderPlacedEvent>(
            reactor,
            transport,
            inbox,
            NullLogger<ReactorHostedService<CountingReactor, OrderPlacedEvent>>.Instance);
        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await WaitForCountingSubscriberAsync(transport);

        var context = new MessageContext { MessageId = "done-1" };
        var evt = new OrderPlacedEvent(Guid.NewGuid(), DateTime.UtcNow, "done");
        await transport.PublishAsync(evt, context);
        await transport.PublishAsync(evt, context);
        Assert.Equal(1, reactor.CallCount);

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    private static async Task WaitForCountingSubscriberAsync(InMemoryMessageTransport transport)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (transport.SubscriberCount<OrderPlacedEvent>() == 0)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Reactor hosted service did not subscribe in time.");
            await Task.Delay(10);
        }
    }

    private sealed class CountingReactor : IReactor<OrderPlacedEvent>
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

    private static ActivityListener EnableAllActivities()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static _ => true,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static async Task WaitForSubscriberAsync(InMemoryMessageTransport transport)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (transport.SubscriberCount<OrderPlacedEvent>() == 0)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Reactor hosted service did not subscribe in time.");
            await Task.Delay(10);
        }
    }
}
