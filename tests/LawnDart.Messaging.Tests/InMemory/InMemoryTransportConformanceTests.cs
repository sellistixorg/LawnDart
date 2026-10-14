using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.Messaging.InMemory;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.Tests.InMemory;

public sealed class InMemoryTransportConformanceTests : MessageTransportConformanceTests
{
    protected override TransportConformanceFixture CreateFixture(
        Action<MessageTransportOptions>? configure = null,
        IMessagePreHandler? preHandler = null)
        => InMemoryTransportFixture.Create(configure, preHandler);

    private sealed class InMemoryTransportFixture : TransportConformanceFixture
    {
        private readonly InMemoryMessageTransport _transport;

        private InMemoryTransportFixture(InMemoryMessageTransport transport, MessageTransportOptions options)
        {
            _transport = transport;
            Options = options;
        }

        public override IMessageTransport Transport => _transport;

        public override MessageTransportOptions Options { get; }

        public override IReadOnlyList<DeadLetteredMessage> DeadLetters => _transport.DeadLetters;

        public static InMemoryTransportFixture Create(
            Action<MessageTransportOptions>? configure,
            IMessagePreHandler? preHandler)
        {
            var options = new MessageTransportOptions();
            configure?.Invoke(options);
            var hooks = preHandler is null ? null : new IMessagePreHandler[] { preHandler };
            var transport = new InMemoryMessageTransport(
                NullLogger<InMemoryMessageTransport>.Instance,
                Microsoft.Extensions.Options.Options.Create(options),
                hooks,
                new MemoryDeadLetterQueue());
            return new InMemoryTransportFixture(transport, options);
        }
    }
}
