namespace LawnDart.Messaging;

/// <summary>
/// Creates topics, queues, or exchanges for the registered message types.
/// </summary>
/// <remarks>
/// The host calls this at startup only when <see cref="MessageTransportOptions.SetupTopology"/>
/// is <c>true</c>. Register an implementation with DI. When the flag is <c>true</c> and no
/// implementation is registered, startup fails.
/// </remarks>
public interface IMessageTopologySetup
{
    /// <summary>
    /// Creates the destinations the transport needs.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetupAsync(CancellationToken cancellationToken);
}
