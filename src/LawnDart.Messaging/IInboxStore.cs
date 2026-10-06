namespace LawnDart.Messaging;

/// <summary>
/// Pluggable inbox deduplication store.
/// Used by reactor and event processor hosted services to guarantee at-most-once handler dispatch
/// despite at-least-once transport delivery.
/// </summary>
public interface IInboxStore
{
    /// <summary>
    /// Returns true if the message with the given ID has already been successfully processed
    /// within the configured deduplication window.
    /// </summary>
    /// <param name="messageId">
    /// Dedup key. Reactor and processor hosted services pass
    /// <c>reactor:{type}:{message id}</c> or <c>processor:{type}:{message id}</c>.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records a message as successfully processed.
    /// Idempotent — calling again for the same ID should not throw.
    /// </summary>
    /// <param name="messageId">
    /// Dedup key. Reactor and processor hosted services pass
    /// <c>reactor:{type}:{message id}</c> or <c>processor:{type}:{message id}</c>.
    /// </param>
    /// <param name="processedAt">When the message was processed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkProcessedAsync(string messageId, DateTimeOffset processedAt, CancellationToken cancellationToken = default);
}
