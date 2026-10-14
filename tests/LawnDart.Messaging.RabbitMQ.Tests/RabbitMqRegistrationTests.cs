using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;

namespace LawnDart.Messaging.RabbitMQ.Tests;

public class RabbitMqRegistrationTests
{
    [Fact]
    public async Task AddRabbitMqTransport_replaces_the_in_memory_transport_and_shares_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryMessaging();
        services.AddRabbitMqTransport(options =>
        {
            options.Uri = "amqp://rabbitmq:rabbitmq@localhost:5672/";
            options.SubscriptionName = "billing";
            options.Prefetch = 20;
            options.SetupTopology = true;
        });

        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IMessageTransport>();
        var setup = provider.GetRequiredService<IMessageTopologySetup>();
        var common = provider.GetRequiredService<IOptions<MessageTransportOptions>>().Value;
        var check = provider.GetServices<IHostedService>().OfType<MessageTransportStartupCheck>().Single();

        Assert.IsType<RabbitMqMessageTransport>(transport);
        Assert.IsType<RabbitMqTopologySetup>(setup);
        Assert.IsType<RabbitMqTransportOptions>(common);
        Assert.Equal("billing", common.SubscriptionName);
        Assert.Equal(20, common.Prefetch);
        Assert.True(common.SetupTopology);
        Assert.NotNull(check);
    }

    [Fact]
    public async Task Startup_check_rejects_a_bad_uri_before_it_connects()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMqTransport(options => options.Uri = "http://localhost/");

        await using var provider = services.BuildServiceProvider();
        var check = provider.GetServices<IHostedService>().OfType<MessageTransportStartupCheck>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => check.StartAsync(CancellationToken.None));
        Assert.Contains("Uri", ex.Message, StringComparison.Ordinal);
    }
}
