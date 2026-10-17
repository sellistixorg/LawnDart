namespace LawnDart.EventStore;

/// <summary>
/// A live subscription stopped because a read or delivery failed.
/// Cancellation and dispose complete the channel without this exception.
/// </summary>
public sealed class EventSubscriptionFailedException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="subscriberId"/>.</summary>
    public EventSubscriptionFailedException(string subscriberId, Exception inner)
        : base($"Subscription '{subscriberId}' failed.", inner)
    {
        SubscriberId = subscriberId;
    }

    /// <summary>Subscriber id passed to <c>Subscribe</c>.</summary>
    public string SubscriberId { get; }
}
