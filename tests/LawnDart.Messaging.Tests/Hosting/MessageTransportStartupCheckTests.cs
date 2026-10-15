using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using LawnDart.EventStore;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Messaging.Tests.Hosting;

public class MessageTransportStartupCheckTests
{
    [Fact]
    public async Task Start_fails_when_the_message_type_has_no_declared_name()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddInMemoryMessaging();
        builder.Services.AddReactor<BareReactor, BareEvent>();

        using var host = builder.Build();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        var text = Flatten(ex);
        Assert.Contains("EventTypeName", text, StringComparison.Ordinal);
        Assert.Contains(typeof(BareEvent).FullName!, text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Start_fails_when_transport_settings_are_invalid()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddInMemoryMessaging();
        builder.Services.Configure<MessageTransportOptions>(options => options.Concurrency = 0);

        using var host = builder.Build();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        Assert.Contains("Concurrency", Flatten(ex), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Start_skips_topology_setup_unless_it_is_enabled()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddInMemoryMessaging();
        builder.Services.AddReactor<OrderPlacedReactor, OrderPlacedEvent>();

        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            var setup = host.Services.GetRequiredService<InMemoryTopologySetup>();
            Assert.Empty(setup.Queues);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task Start_creates_queues_when_topology_setup_is_enabled()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddInMemoryMessaging();
        builder.Services.Configure<MessageTransportOptions>(options =>
        {
            options.SetupTopology = true;
            options.SubscriptionName = "billing";
        });
        builder.Services.AddReactor<OrderPlacedReactor, OrderPlacedEvent>();

        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            var setup = host.Services.GetRequiredService<InMemoryTopologySetup>();
            Assert.Equal(["test-types.order-placed-event/billing"], setup.Queues);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task Start_fails_when_topology_setup_is_enabled_and_missing()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddMessaging();
        builder.Services.Configure<MessageTransportOptions>(options => options.SetupTopology = true);

        using var host = builder.Build();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
        Assert.Contains("IMessageTopologySetup", Flatten(ex), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Start_rejects_declared_names_that_sanitize_to_one_topic()
    {
        var check = new MessageTransportStartupCheck(
            Options.Create(new MessageTransportOptions
            {
                TopicMapper = new SanitizingTopicMapper(),
            }),
            [new MessageSubscription(typeof(SanitizeDotted)), new MessageSubscription(typeof(SanitizeHyphen))],
            []);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => check.StartAsync(CancellationToken.None));
        Assert.Contains("sanitize-order", ex.Message, StringComparison.Ordinal);
    }

    private static string Flatten(Exception exception)
    {
        var text = exception.Message;
        for (var inner = exception.InnerException; inner is not null; inner = inner.InnerException)
            text += " " + inner.Message;
        return text;
    }

    [EventTypeName("sanitize.order")]
    private sealed record SanitizeDotted(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("sanitize-order")]
    private sealed record SanitizeHyphen(Guid Id, DateTime Timestamp) : IEvent;

    private sealed record BareEvent(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class BareReactor : IReactor<BareEvent>
    {
        public Task<IEnumerable<ICommand>> ReactAsync(
            BareEvent @event,
            MessageContext context,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<ICommand>>([]);
    }
}
