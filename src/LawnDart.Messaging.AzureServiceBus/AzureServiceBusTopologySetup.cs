using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// Creates a topic and one subscription for each message type the host recorded.
/// </summary>
public sealed class AzureServiceBusTopologySetup : IMessageTopologySetup
{
    private readonly IReadOnlyList<MessageSubscription> _subscriptions;
    private readonly AzureServiceBusTransportOptions _options;
    private readonly AzureServiceBusConnector _connector;
    private readonly List<string> _topics = [];

    /// <summary>
    /// Creates the setup from the host's subscriptions and Service Bus settings.
    /// </summary>
    /// <param name="subscriptions">Message types recorded by reactor and processor registration.</param>
    /// <param name="options">Connection and subscription settings.</param>
    /// <param name="connector">Shared Service Bus clients.</param>
    internal AzureServiceBusTopologySetup(
        IEnumerable<MessageSubscription> subscriptions,
        IOptions<AzureServiceBusTransportOptions> options,
        AzureServiceBusConnector connector)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connector);
        _subscriptions = subscriptions.ToArray();
        _options = options.Value;
        _connector = connector;
    }

    /// <summary>Topic names created by the last <see cref="SetupAsync"/> call.</summary>
    public IReadOnlyList<string> Topics => _topics;

    /// <inheritdoc />
    public async Task SetupAsync(CancellationToken cancellationToken)
    {
        _options.EnsureValid();
        foreach (var subscription in _subscriptions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var messageType = MessageTypeNames.Require(subscription.MessageType);
            var topic = AzureServiceBusNames.Topic(messageType);
            await _connector.EnsureSubscriptionAsync(topic, _options.SubscriptionName, cancellationToken)
                .ConfigureAwait(false);

            if (!_topics.Contains(topic, StringComparer.Ordinal))
                _topics.Add(topic);
        }
    }
}
