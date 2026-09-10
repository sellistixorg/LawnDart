using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.Messaging;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;

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
