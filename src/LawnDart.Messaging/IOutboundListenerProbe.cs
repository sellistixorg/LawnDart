namespace LawnDart.Messaging;

/// <summary>
/// Asks a transport whether a published message type currently has a listener.
/// The in-process transport counts subscribers in this process.
/// A broker transport counts a queue that can receive the message.
/// </summary>
internal interface IOutboundListenerProbe
{
    /// <summary>
    /// Returns true when <paramref name="messageType"/> has a listener.
    /// </summary>
    Task<bool> HasListenerAsync(Type messageType, CancellationToken cancellationToken);
}
