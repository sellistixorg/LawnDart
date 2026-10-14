using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.AzureServiceBus.Tests;

[Collection(AzureServiceBusCollection.Name)]
[Trait("Category", "Integration")]
public sealed class AzureServiceBusTransportConformanceTests : MessageTransportConformanceTests
{
    private readonly AzureServiceBusContainerFixture _container;

    public AzureServiceBusTransportConformanceTests(AzureServiceBusContainerFixture container) => _container = container;

    protected override TransportConformanceFixture CreateFixture(
        Action<MessageTransportOptions>? configure = null,
        IMessagePreHandler? preHandler = null)
        => AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            configure,
            preHandler);
}

internal sealed class AzureServiceBusTransportFixture : TransportConformanceFixture
{
    private readonly AzureServiceBusMessageTransport _transport;

    private AzureServiceBusTransportFixture(AzureServiceBusMessageTransport transport, AzureServiceBusTransportOptions options)
    {
        _transport = transport;
        Options = options;
    }

    public override IMessageTransport Transport => _transport;

    public override MessageTransportOptions Options { get; }

    public override IReadOnlyList<DeadLetteredMessage> DeadLetters => _transport.DeadLetters;

    public static AzureServiceBusTransportFixture Create(
        string amqpConnectionString,
        string adminConnectionString,
        Action<MessageTransportOptions>? configure,
        IMessagePreHandler? preHandler,
        ILogger<AzureServiceBusMessageTransport>? logger = null)
    {
        var options = new AzureServiceBusTransportOptions
        {
            ConnectionString = amqpConnectionString,
            AdministrationConnectionString = adminConnectionString,
            RetryDelay = TimeSpan.FromMilliseconds(200),
            BrokerMaxDeliveryCount = 10,
        };
        var subscription = options.SubscriptionName;
        configure?.Invoke(options);
        if (options.SubscriptionName == subscription)
            options.SubscriptionName = subscription + "-" + Guid.NewGuid().ToString("N")[..8];

        var hooks = preHandler is null ? null : new IMessagePreHandler[] { preHandler };
        var transport = new AzureServiceBusMessageTransport(
            logger ?? NullLogger<AzureServiceBusMessageTransport>.Instance,
            Microsoft.Extensions.Options.Options.Create(options),
            hooks,
            new MemoryDeadLetterQueue());
        return new AzureServiceBusTransportFixture(transport, options);
    }

    public override ValueTask DisposeAsync() => _transport.DisposeAsync();
}
