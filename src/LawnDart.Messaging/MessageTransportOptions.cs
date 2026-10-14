namespace LawnDart.Messaging;

/// <summary>
/// Common settings for a message transport.
/// A transport package can subclass this and add connection settings.
/// </summary>
public class MessageTransportOptions
{
    /// <summary>
    /// Consumer name bound to each message type.
    /// Defaults to <c>lawndart</c>.
    /// </summary>
    public string SubscriptionName { get; set; } = "lawndart";

    /// <summary>
    /// How many messages a transport may handle at the same time.
    /// Must be at least 1. Defaults to 1.
    /// </summary>
    public int Concurrency { get; set; } = 1;

    /// <summary>
    /// How many messages a transport may fetch before the handlers finish.
    /// Must be zero or greater. Defaults to 10.
    /// </summary>
    public int Prefetch { get; set; } = 10;

    /// <summary>
    /// How many times a handler may run for one message, including the first attempt.
    /// Must be at least 1. Defaults to 5.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>
    /// When <c>true</c>, a message that still fails after <see cref="MaxDeliveryAttempts"/>
    /// is dead-lettered. When <c>false</c>, the last failure is thrown and the message is not parked.
    /// Defaults to <c>false</c>, so a caller such as the outbox still sees the failure.
    /// </summary>
    public bool DeadLetterFailedMessages { get; set; }

    /// <summary>
    /// When <c>true</c>, host startup runs <see cref="IMessageTopologySetup"/>.
    /// When <c>false</c>, startup skips that step. Defaults to <c>false</c>.
    /// </summary>
    public bool SetupTopology { get; set; }

    /// <summary>
    /// Throws when a setting is outside the range a transport can use.
    /// A subclass that adds settings should call this method and then check its own.
    /// </summary>
    public virtual void EnsureValid()
    {
        if (string.IsNullOrWhiteSpace(SubscriptionName))
        {
            throw new InvalidOperationException(
                "MessageTransportOptions.SubscriptionName is required.");
        }

        if (Concurrency < 1)
        {
            throw new InvalidOperationException(
                "MessageTransportOptions.Concurrency must be at least 1.");
        }

        if (Prefetch < 0)
        {
            throw new InvalidOperationException(
                "MessageTransportOptions.Prefetch must be zero or greater.");
        }

        if (MaxDeliveryAttempts < 1)
        {
            throw new InvalidOperationException(
                "MessageTransportOptions.MaxDeliveryAttempts must be at least 1.");
        }
    }
}
