using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// Creates an SNS topic and one SQS queue for each message type the host recorded.
/// Types that map to the same topic share that topic. The queue's filter lists those types.
/// </summary>
public sealed class AwsSqsTopologySetup : IMessageTopologySetup
{
    private readonly IReadOnlyList<MessageSubscription> _subscriptions;
    private readonly AwsSqsTransportOptions _options;
    private readonly AwsSqsConnector _connector;
    private readonly List<string> _topics = [];

    /// <summary>
    /// Creates the setup from the host's subscriptions and SQS settings.
    /// </summary>
    /// <param name="subscriptions">Message types recorded by reactor and processor registration.</param>
    /// <param name="options">Connection and subscription settings.</param>
    /// <param name="connector">Shared SQS and SNS clients.</param>
    internal AwsSqsTopologySetup(
        IEnumerable<MessageSubscription> subscriptions,
        IOptions<AwsSqsTransportOptions> options,
        AwsSqsConnector connector)
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
            AwsSqsNames.RequireTopic(topic);
            if (!groups.TryGetValue(topic, out var types))
            {
                types = [];
                groups[topic] = types;
            }

            if (!types.Contains(messageType, StringComparer.Ordinal))
                types.Add(messageType);
        }

        if (mapper is SanitizingTopicMapper sanitizing)
            sanitizing.EnsureNoCollisions(groups.SelectMany(static pair => pair.Value));

        foreach (var (topic, types) in groups)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _connector.EnsureSubscriptionAsync(
                    topic,
                    _options.SubscriptionName,
                    types,
                    _options.ReplaceRouteRule,
                    _options.DeadLetterFailedMessages,
                    cancellationToken)
                .ConfigureAwait(false);

            if (!_topics.Contains(topic, StringComparer.Ordinal))
                _topics.Add(topic);
        }
    }
}
