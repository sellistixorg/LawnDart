using Testcontainers.ServiceBus;

namespace LawnDart.Messaging.AzureServiceBus.Tests;

public sealed class AzureServiceBusContainerFixture : IAsyncLifetime
{
    private readonly ServiceBusContainer _container = new ServiceBusBuilder(
            "mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.1")
        .WithAcceptLicenseAgreement(true)
        .Build();

    public string AmqpConnectionString { get; private set; } = string.Empty;

    public string AdminConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        AmqpConnectionString = _container.GetConnectionString();
        AdminConnectionString = _container.GetHttpConnectionString();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class AzureServiceBusCollection : ICollectionFixture<AzureServiceBusContainerFixture>
{
    public const string Name = "AzureServiceBus";
}
