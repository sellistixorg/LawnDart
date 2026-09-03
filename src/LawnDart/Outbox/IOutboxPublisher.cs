namespace LawnDart.Outbox;

/// <summary>
/// Interface for publishing outbox messages to external systems.
/// Implementations handle the actual delivery mechanism (e.g., message queue, HTTP, etc.).
/// </summary>
public interface IOutboxPublisher
{
    /// <summary>
    /// Publishes an outbox message to the external system.
    /// </summary>
    /// <param name="message">Outbox message to publish.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default);
}
