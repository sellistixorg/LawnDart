namespace LawnDart.Messaging;

/// <summary>
/// A message parked after its delivery attempts were used.
/// </summary>
public sealed class DeadLetteredMessage
{
    /// <summary>
    /// Records one parked message.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    /// <param name="message">Message body.</param>
    /// <param name="context">Context from the failed delivery.</param>
    /// <param name="attempts">How many times the handler ran.</param>
    /// <param name="error">Failure text, usually the exception type and message.</param>
    public DeadLetteredMessage(
        string messageType,
        object message,
        MessageContext context,
        int attempts,
        string error)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentOutOfRangeException.ThrowIfLessThan(attempts, 1);
        ArgumentException.ThrowIfNullOrWhiteSpace(error);

        MessageType = messageType;
        Message = message;
        Context = context;
        Attempts = attempts;
        Error = error;
    }

    /// <summary>Declared message type name.</summary>
    public string MessageType { get; }

    /// <summary>Message body that failed.</summary>
    public object Message { get; }

    /// <summary>Context from the failed delivery.</summary>
    public MessageContext Context { get; }

    /// <summary>How many times the handler ran.</summary>
    public int Attempts { get; }

    /// <summary>Failure text from the last attempt.</summary>
    public string Error { get; }
}

/// <summary>
/// Holds messages that failed past <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>
/// when dead-letter handling is on.
/// </summary>
public interface IDeadLetterQueue
{
    /// <summary>Parks <paramref name="message"/>.</summary>
    /// <param name="message">The parked message.</param>
    void Enqueue(DeadLetteredMessage message);

    /// <summary>Returns the messages parked so far, oldest first.</summary>
    IReadOnlyList<DeadLetteredMessage> ReadAll();
}

/// <summary>
/// In-process <see cref="IDeadLetterQueue"/>. Safe to call from more than one thread.
/// </summary>
public sealed class MemoryDeadLetterQueue : IDeadLetterQueue
{
    private readonly Lock _gate = new();
    private readonly List<DeadLetteredMessage> _items = [];

    /// <inheritdoc />
    public void Enqueue(DeadLetteredMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        lock (_gate)
            _items.Add(message);
    }

    /// <inheritdoc />
    public IReadOnlyList<DeadLetteredMessage> ReadAll()
    {
        lock (_gate)
            return _items.ToArray();
    }
}
