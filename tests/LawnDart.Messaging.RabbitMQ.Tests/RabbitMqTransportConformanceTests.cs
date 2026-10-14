using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.RabbitMQ.Tests;

[Collection(RabbitMqCollection.Name)]
[Trait("Category", "Integration")]
public sealed class RabbitMqTransportConformanceTests : MessageTransportConformanceTests
{
    private readonly RabbitMqContainerFixture _container;

    public RabbitMqTransportConformanceTests(RabbitMqContainerFixture container) => _container = container;

    protected override TransportConformanceFixture CreateFixture(
        Action<MessageTransportOptions>? configure = null,
        IMessagePreHandler? preHandler = null)
        => RabbitMqTransportFixture.Create(_container.AmqpUri, configure, preHandler);
}

internal sealed class RabbitMqTransportFixture : TransportConformanceFixture
{
    private readonly RabbitMqMessageTransport _transport;

    private RabbitMqTransportFixture(RabbitMqMessageTransport transport, RabbitMqTransportOptions options)
    {
        _transport = transport;
        Options = options;
    }

    public override IMessageTransport Transport => _transport;

    public override MessageTransportOptions Options { get; }

    public override IReadOnlyList<DeadLetteredMessage> DeadLetters => _transport.DeadLetters;

    public static RabbitMqTransportFixture Create(
        string amqpUri,
        Action<MessageTransportOptions>? configure,
        IMessagePreHandler? preHandler,
        ILogger<RabbitMqMessageTransport>? logger = null)
    {
        var options = new RabbitMqTransportOptions { Uri = amqpUri };
        var subscription = options.SubscriptionName;
        configure?.Invoke(options);
        if (options.SubscriptionName == subscription)
            options.SubscriptionName = subscription + "-" + Guid.NewGuid().ToString("N")[..8];

        var hooks = preHandler is null ? null : new IMessagePreHandler[] { preHandler };
        var transport = new RabbitMqMessageTransport(
            logger ?? NullLogger<RabbitMqMessageTransport>.Instance,
            Microsoft.Extensions.Options.Options.Create(options),
            hooks,
            new MemoryDeadLetterQueue());
        return new RabbitMqTransportFixture(transport, options);
    }

    public override ValueTask DisposeAsync() => _transport.DisposeAsync();
}
