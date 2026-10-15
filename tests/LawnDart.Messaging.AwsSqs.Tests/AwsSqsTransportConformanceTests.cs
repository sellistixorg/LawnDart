using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.AwsSqs.Tests;

[Collection(AwsSqsCollection.Name)]
[Trait("Category", "Integration")]
public sealed class AwsSqsTransportConformanceTests : MessageTransportConformanceTests
{
    private readonly AwsSqsContainerFixture _container;

    public AwsSqsTransportConformanceTests(AwsSqsContainerFixture container) => _container = container;

    protected override TransportConformanceFixture CreateFixture(
        Action<MessageTransportOptions>? configure = null,
        IMessagePreHandler? preHandler = null)
        => AwsSqsTransportFixture.Create(_container.ServiceUrl, configure, preHandler);
}

internal sealed class AwsSqsTransportFixture : TransportConformanceFixture
{
    private readonly AwsSqsMessageTransport _transport;

    private AwsSqsTransportFixture(AwsSqsMessageTransport transport, AwsSqsTransportOptions options)
    {
        _transport = transport;
        Options = options;
    }

    public override IMessageTransport Transport => _transport;

    public override MessageTransportOptions Options { get; }

    public override IReadOnlyList<DeadLetteredMessage> DeadLetters => _transport.DeadLetters;

    public static AwsSqsTransportFixture Create(
        string serviceUrl,
        Action<MessageTransportOptions>? configure,
        IMessagePreHandler? preHandler,
        ILogger<AwsSqsMessageTransport>? logger = null,
        IEnumerable<MessageSubscription>? subscriptions = null)
    {
        var options = new AwsSqsTransportOptions
        {
            ServiceUrl = serviceUrl,
            Region = "us-east-1",
            RetryDelay = TimeSpan.FromMilliseconds(200),
            OperationTimeout = TimeSpan.FromSeconds(60),
        };
        var subscription = options.SubscriptionName;
        configure?.Invoke(options);
        if (options.SubscriptionName == subscription)
            options.SubscriptionName = subscription + "-" + Guid.NewGuid().ToString("N")[..8];

        var inner = options.TopicMapper ?? DeclaredTypeTopicMapper.Instance;
        options.TopicMapper = new DelegateTopicMapper(name => inner.TopicFor(name).Replace('.', '-'));

        var hooks = preHandler is null ? null : new IMessagePreHandler[] { preHandler };
        var transport = new AwsSqsMessageTransport(
            logger ?? NullLogger<AwsSqsMessageTransport>.Instance,
            Microsoft.Extensions.Options.Options.Create(options),
            hooks,
            new MemoryDeadLetterQueue(),
            subscriptions);
        return new AwsSqsTransportFixture(transport, options);
    }

    public override ValueTask DisposeAsync() => _transport.DisposeAsync();
}
