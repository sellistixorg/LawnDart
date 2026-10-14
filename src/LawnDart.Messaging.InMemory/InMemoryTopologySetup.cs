using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.InMemory;

/// <summary>
/// Records an in-process queue for each registered message type.
/// The name is the declared type name, a slash, and the subscription name.
/// </summary>
public sealed class InMemoryTopologySetup : IMessageTopologySetup
{
    private readonly IReadOnlyList<MessageSubscription> _subscriptions;
    private readonly MessageTransportOptions _options;
    private readonly List<string> _queues = [];

    /// <summary>
    /// Creates the setup from the host's subscriptions and transport settings.
    /// </summary>
    /// <param name="subscriptions">Message types recorded by reactor and processor registration.</param>
    /// <param name="options">Common transport settings. <see cref="MessageTransportOptions.SubscriptionName"/> is part of each queue name.</param>
    public InMemoryTopologySetup(
        IEnumerable<MessageSubscription> subscriptions,
        IOptions<MessageTransportOptions> options)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(options);
        _subscriptions = subscriptions.ToArray();
        _options = options.Value;
    }

    /// <summary>Queue names created by the last <see cref="SetupAsync"/> call.</summary>
    public IReadOnlyList<string> Queues => _queues;

    /// <inheritdoc />
    public Task SetupAsync(CancellationToken cancellationToken)
    {
        foreach (var subscription in _subscriptions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var topic = MessageTypeNames.Require(subscription.MessageType);
            var queue = topic + "/" + _options.SubscriptionName;
            if (!_queues.Contains(queue, StringComparer.Ordinal))
                _queues.Add(queue);
        }

        return Task.CompletedTask;
    }
}
