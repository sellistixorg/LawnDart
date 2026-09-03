namespace LawnDart.EventStore;

/// <summary>
/// Live subscription handle returned by <see cref="IEventStoreSubscriptions.Subscribe"/>.
/// Dispose or cancel to unsubscribe and release channel resources.
/// </summary>
public interface ISubscriptionHandle : IAsyncDisposable, IDisposable
{
    /// <summary>Stable identifier for this subscription (logging / metrics).</summary>
    string SubscriberId { get; }

    /// <summary>
    /// Channel reader from which the consumer reads delivered <see cref="SequencedEvent"/> values
    /// in global sequence order.
    /// </summary>
    System.Threading.Channels.ChannelReader<SequencedEvent> Events { get; }

    /// <summary>
    /// Advisory last sequence written to the channel by the producer.
    /// The client owns the durable cursor used for reconnect; do not treat this as a
    /// server-persisted checkpoint under the portable contract.
    /// </summary>
    long LastDeliveredSequence { get; }
}
