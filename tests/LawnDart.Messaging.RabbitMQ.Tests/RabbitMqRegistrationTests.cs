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

    [Fact]
    public void Common_transport_options_merge_into_the_rabbit_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<MessageTransportOptions>(options =>
        {
            options.SubscriptionName = "common";
            options.Prefetch = 8;
            options.MaxDeliveryAttempts = 2;
        });
        services.AddRabbitMqTransport(options =>
        {
            options.Uri = "amqp://rabbitmq:rabbitmq@localhost:5672/";
            options.SubscriptionName = "rabbit";
        });
        services.Configure<MessageTransportOptions>(options => options.Concurrency = 3);

        using var provider = services.BuildServiceProvider();
        var rabbit = provider.GetRequiredService<IOptions<RabbitMqTransportOptions>>().Value;
        var common = provider.GetRequiredService<IOptions<MessageTransportOptions>>().Value;

        Assert.Equal("rabbit", rabbit.SubscriptionName);
        Assert.Equal(8, rabbit.Prefetch);
        Assert.Equal(2, rabbit.MaxDeliveryAttempts);
        Assert.Equal(3, rabbit.Concurrency);
        Assert.Equal("rabbit", common.SubscriptionName);
        Assert.Equal(8, common.Prefetch);
    }

    [Fact]
    public void A_registered_topic_mapper_is_used_when_options_keep_the_default()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ITopicMapper>(new SharedTopicMapper("from-di"));
        services.AddRabbitMqTransport(options =>
            options.Uri = "amqp://rabbitmq:rabbitmq@localhost:5672/");

        using var provider = services.BuildServiceProvider();
        var rabbit = provider.GetRequiredService<IOptions<RabbitMqTransportOptions>>().Value;
        Assert.Equal("from-di", rabbit.TopicMapper.TopicFor("order-placed"));
    }

    [Fact]
    public void A_mapper_set_on_options_wins_over_a_registered_mapper()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ITopicMapper>(new SharedTopicMapper("from-di"));
        services.AddRabbitMqTransport(options =>
        {
            options.Uri = "amqp://rabbitmq:rabbitmq@localhost:5672/";
            options.TopicMapper = new SharedTopicMapper("from-options");
        });

        using var provider = services.BuildServiceProvider();
        var rabbit = provider.GetRequiredService<IOptions<RabbitMqTransportOptions>>().Value;
        Assert.Equal("from-options", rabbit.TopicMapper.TopicFor("order-placed"));
    }
}
