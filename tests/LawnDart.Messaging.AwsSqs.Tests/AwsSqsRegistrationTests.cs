using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;

namespace LawnDart.Messaging.AwsSqs.Tests;

public class AwsSqsRegistrationTests
{
    [Fact]
    public async Task AddAwsSqsTransport_replaces_the_in_memory_transport_and_shares_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryMessaging();
        services.AddSingleton(TestSqs());
        services.AddSingleton(TestSns());
        services.AddAwsSqsTransport(options =>
        {
            options.SubscriptionName = "billing";
            options.Prefetch = 20;
            options.SetupTopology = true;
        });

        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IMessageTransport>();
        var setup = provider.GetRequiredService<IMessageTopologySetup>();
        var common = provider.GetRequiredService<IOptions<MessageTransportOptions>>().Value;
        var check = provider.GetServices<IHostedService>().OfType<MessageTransportStartupCheck>().Single();

        Assert.IsType<AwsSqsMessageTransport>(transport);
        Assert.IsType<AwsSqsTopologySetup>(setup);
        Assert.IsType<AwsSqsTransportOptions>(common);
        Assert.Equal("billing", common.SubscriptionName);
        Assert.Equal(20, common.Prefetch);
        Assert.True(common.SetupTopology);
        Assert.NotNull(check);
    }

    [Fact]
    public async Task Startup_check_rejects_a_bad_service_url_before_it_connects()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAwsSqsTransport(options => options.ServiceUrl = "ftp://localhost");

        await using var provider = services.BuildServiceProvider();
        var check = provider.GetServices<IHostedService>().OfType<MessageTransportStartupCheck>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => check.StartAsync(CancellationToken.None));
        Assert.Contains("ServiceUrl", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Common_transport_options_merge_into_the_sqs_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<MessageTransportOptions>(options =>
        {
            options.SubscriptionName = "common";
            options.Prefetch = 8;
            options.MaxDeliveryAttempts = 2;
        });
        services.AddAwsSqsTransport(options =>
        {
            options.Region = "us-east-1";
            options.SubscriptionName = "sqs";
        });
        services.Configure<MessageTransportOptions>(options => options.Concurrency = 3);

        using var provider = services.BuildServiceProvider();
        var sqs = provider.GetRequiredService<IOptions<AwsSqsTransportOptions>>().Value;
        var common = provider.GetRequiredService<IOptions<MessageTransportOptions>>().Value;

        Assert.Equal("sqs", sqs.SubscriptionName);
        Assert.Equal(8, sqs.Prefetch);
        Assert.Equal(2, sqs.MaxDeliveryAttempts);
        Assert.Equal(3, sqs.Concurrency);
        Assert.Equal("sqs", common.SubscriptionName);
        Assert.Equal(8, common.Prefetch);
    }

    [Fact]
    public void A_registered_client_is_kept()
    {
        var sqs = TestSqs();
        var sns = TestSns();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(sqs);
        services.AddSingleton(sns);
        services.AddAwsSqsTransport(options => options.SubscriptionName = "billing");

        using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IMessageTransport>();

        Assert.IsType<AwsSqsMessageTransport>(transport);
        Assert.Same(sqs, provider.GetRequiredService<IAmazonSQS>());
        Assert.Same(sns, provider.GetRequiredService<IAmazonSimpleNotificationService>());
    }

    [Fact]
    public void A_registered_topic_mapper_is_used_when_options_keep_the_default()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ITopicMapper>(new SharedTopicMapper("from-di"));
        services.AddAwsSqsTransport(options => options.Region = "us-east-1");

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<AwsSqsTransportOptions>>().Value;
        Assert.Equal("from-di", options.TopicMapper.TopicFor("order-placed"));
    }

    [Fact]
    public void A_mapper_set_on_options_wins_over_a_registered_mapper()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ITopicMapper>(new SharedTopicMapper("from-di"));
        services.AddAwsSqsTransport(options =>
        {
            options.Region = "us-east-1";
            options.TopicMapper = new SharedTopicMapper("from-options");
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<AwsSqsTransportOptions>>().Value;
        Assert.Equal("from-options", options.TopicMapper.TopicFor("order-placed"));
    }

    private static IAmazonSQS TestSqs()
        => new AmazonSQSClient(
            new BasicAWSCredentials("test", "test"),
            new AmazonSQSConfig { ServiceURL = "http://127.0.0.1:4566", AuthenticationRegion = "us-east-1" });

    private static IAmazonSimpleNotificationService TestSns()
        => new AmazonSimpleNotificationServiceClient(
            new BasicAWSCredentials("test", "test"),
            new AmazonSimpleNotificationServiceConfig
            {
                ServiceURL = "http://127.0.0.1:4566",
                AuthenticationRegion = "us-east-1",
            });
}
