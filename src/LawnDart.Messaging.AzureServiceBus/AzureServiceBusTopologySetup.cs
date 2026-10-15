using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// Creates a topic and one subscription for each message type the host recorded.
/// Types that map to the same topic share that topic. The subscription rule lists those types.
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
        var mapper = TopicMapperSelection.FromOptions(_options);
        var groups = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var subscription in _subscriptions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var messageType = MessageTypeNames.Require(subscription.MessageType);
            var topic = mapper.TopicFor(messageType);
            AzureServiceBusNames.RequireTopic(topic);
            if (!groups.TryGetValue(topic, out var types))
            {
                types = [];
                groups[topic] = types;
            }

            if (!types.Contains(messageType, StringComparer.Ordinal))
                types.Add(messageType);
        }

        foreach (var (topic, types) in groups)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _connector.EnsureSubscriptionAsync(
                    topic, _options.SubscriptionName, types, _options.ReplaceRouteRule, cancellationToken)
                .ConfigureAwait(false);

            if (!_topics.Contains(topic, StringComparer.Ordinal))
                _topics.Add(topic);
        }
    }
}
