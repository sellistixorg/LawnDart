namespace LawnDart.Messaging;

/// <summary>
/// Asks a transport whether a published message type currently has a listener.
/// The in-process transport counts subscribers in this process.
/// A queue transport that implements this counts a live subscription and does not create the topic.
/// A transport that cannot tell does not implement this probe.
/// </summary>
internal interface IOutboundListenerProbe
{
    /// <summary>
    /// Returns true when <paramref name="messageType"/> has a listener.
    /// </summary>
    Task<bool> HasListenerAsync(Type messageType, CancellationToken cancellationToken);
}
