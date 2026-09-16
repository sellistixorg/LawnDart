namespace LawnDart.EventStore;

/// <summary>
/// Live subscription handle returned by <see cref="IEventLogSubscriptions.Subscribe"/>.
/// </summary>
public interface IEventLogSubscriptionHandle : IAsyncDisposable, IDisposable
{
    /// <summary>Stable identifier for this subscription (logging / metrics).</summary>
    string SubscriberId { get; }

    /// <summary>
    /// Channel reader of recorded frames in non-decreasing
    /// <see cref="RecordedEvent.SequencePosition"/> order.
    /// </summary>
    System.Threading.Channels.ChannelReader<RecordedEvent> Events { get; }

    /// <summary>
    /// Advisory last sequence written to the channel by the producer.
    /// Not a server-persisted checkpoint under the portable contract.
    /// </summary>
    long LastDeliveredSequence { get; }
}
