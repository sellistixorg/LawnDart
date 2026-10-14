using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;

namespace LawnDart.Messaging.AzureServiceBus.Tests;

public class AzureServiceBusRegistrationTests
{
    private const string ConnectionString =
        "Endpoint=sb://example.servicebus.windows.net;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE";

    [Fact]
    public async Task AddAzureServiceBusTransport_replaces_the_in_memory_transport_and_shares_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryMessaging();
        services.AddAzureServiceBusTransport(options =>
        {
            options.ConnectionString = ConnectionString;
            options.SubscriptionName = "billing";
            options.Prefetch = 20;
            options.SetupTopology = true;
        });

        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IMessageTransport>();
        var setup = provider.GetRequiredService<IMessageTopologySetup>();
        var common = provider.GetRequiredService<IOptions<MessageTransportOptions>>().Value;
        var check = provider.GetServices<IHostedService>().OfType<MessageTransportStartupCheck>().Single();

        Assert.IsType<AzureServiceBusMessageTransport>(transport);
        Assert.IsType<AzureServiceBusTopologySetup>(setup);
        Assert.IsType<AzureServiceBusTransportOptions>(common);
        Assert.Equal("billing", common.SubscriptionName);
        Assert.Equal(20, common.Prefetch);
        Assert.True(common.SetupTopology);
        Assert.NotNull(check);
    }

    [Fact]
    public async Task Startup_check_rejects_a_bad_connection_string_before_it_connects()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAzureServiceBusTransport(options => options.ConnectionString = "Endpoint=http://localhost/");

        await using var provider = services.BuildServiceProvider();
        var check = provider.GetServices<IHostedService>().OfType<MessageTransportStartupCheck>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => check.StartAsync(CancellationToken.None));
        Assert.Contains("ConnectionString", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Common_transport_options_merge_into_the_service_bus_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<MessageTransportOptions>(options =>
        {
            options.SubscriptionName = "common";
            options.Prefetch = 8;
            options.MaxDeliveryAttempts = 2;
        });
        services.AddAzureServiceBusTransport(options =>
        {
            options.ConnectionString = ConnectionString;
            options.SubscriptionName = "servicebus";
        });
        services.Configure<MessageTransportOptions>(options => options.Concurrency = 3);

        using var provider = services.BuildServiceProvider();
        var serviceBus = provider.GetRequiredService<IOptions<AzureServiceBusTransportOptions>>().Value;
        var common = provider.GetRequiredService<IOptions<MessageTransportOptions>>().Value;

        Assert.Equal("servicebus", serviceBus.SubscriptionName);
        Assert.Equal(8, serviceBus.Prefetch);
        Assert.Equal(2, serviceBus.MaxDeliveryAttempts);
        Assert.Equal(3, serviceBus.Concurrency);
        Assert.Equal("servicebus", common.SubscriptionName);
        Assert.Equal(8, common.Prefetch);
    }

    [Fact]
    public void A_registered_client_is_kept()
    {
        var client = new ServiceBusClient(ConnectionString);
        var admin = new ServiceBusAdministrationClient(ConnectionString);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(client);
        services.AddSingleton(admin);
        services.AddAzureServiceBusTransport(options => options.SubscriptionName = "billing");

        using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IMessageTransport>();

        Assert.IsType<AzureServiceBusMessageTransport>(transport);
        Assert.Same(client, provider.GetRequiredService<ServiceBusClient>());
        Assert.Same(admin, provider.GetRequiredService<ServiceBusAdministrationClient>());
    }
}
