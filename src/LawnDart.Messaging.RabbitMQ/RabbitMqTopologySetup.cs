using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Declares a direct exchange, a quorum queue, and a dead-letter exchange and queue
/// for each message type the host recorded.
/// </summary>
public sealed class RabbitMqTopologySetup : IMessageTopologySetup
{
    private readonly IReadOnlyList<MessageSubscription> _subscriptions;
    private readonly RabbitMqTransportOptions _options;
    private readonly RabbitMqConnector _connector;
    private readonly List<string> _queues = [];

    /// <summary>
    /// Creates the setup from the host's subscriptions and RabbitMQ settings.
    /// </summary>
    /// <param name="subscriptions">Message types recorded by reactor and processor registration.</param>
    /// <param name="options">Connection and subscription settings.</param>
    /// <param name="connector">Shared broker connection.</param>
    internal RabbitMqTopologySetup(
        IEnumerable<MessageSubscription> subscriptions,
        IOptions<RabbitMqTransportOptions> options,
        RabbitMqConnector connector)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connector);
        _subscriptions = subscriptions.ToArray();
        _options = options.Value;
        _connector = connector;
    }

    /// <summary>Queue names created by the last <see cref="SetupAsync"/> call.</summary>
    public IReadOnlyList<string> Queues => _queues;

    /// <inheritdoc />
    public async Task SetupAsync(CancellationToken cancellationToken)
    {
        _options.EnsureValid();
        foreach (var subscription in _subscriptions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var messageType = MessageTypeNames.Require(subscription.MessageType);
            await _connector.DeclareSubscriptionAsync(messageType, _options.SubscriptionName, cancellationToken)
                .ConfigureAwait(false);

            var queue = RabbitMqNames.Queue(messageType, _options.SubscriptionName);
            if (!_queues.Contains(queue, StringComparer.Ordinal))
                _queues.Add(queue);
        }
    }
}
