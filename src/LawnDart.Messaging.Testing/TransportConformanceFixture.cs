namespace LawnDart.Messaging.Testing;

/// <summary>
/// What one conformance test needs from a transport: a way to publish, the settings in force,
/// and the messages that were dead-lettered.
/// </summary>
public abstract class TransportConformanceFixture : IAsyncDisposable
{
    /// <summary>Transport under test.</summary>
    public abstract IMessageTransport Transport { get; }

    /// <summary>Settings the transport is using for this fixture.</summary>
    public abstract MessageTransportOptions Options { get; }

    /// <summary>Messages parked after they used their delivery attempts.</summary>
    public abstract IReadOnlyList<DeadLetteredMessage> DeadLetters { get; }

    /// <summary>Releases the transport. The default does nothing.</summary>
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
