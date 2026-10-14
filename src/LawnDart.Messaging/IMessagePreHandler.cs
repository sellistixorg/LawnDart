namespace LawnDart.Messaging;

/// <summary>
/// Runs before the message handler on each delivery attempt.
/// </summary>
/// <remarks>
/// Register with <see cref="MessagingExtensions.AddMessagePreHandler{THandler}"/>.
/// The transport invokes every registration, in registration order, and then the handler.
/// A failure here counts as a failed attempt.
/// </remarks>
public interface IMessagePreHandler
{
    /// <summary>
    /// Called immediately before the message handler.
    /// </summary>
    /// <param name="context">Context for this delivery. The same instance the handler receives.</param>
    /// <param name="cancellationToken">Cancellation token for this attempt.</param>
    Task BeforeHandleAsync(MessageContext context, CancellationToken cancellationToken);
}
