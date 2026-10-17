namespace LawnDart.Messaging;

/// <summary>
/// Pluggable inbox deduplication store.
/// Used by reactor and event processor hosted services so two deliveries of the same
/// message do not both run the handler. A claim is held while the handler runs.
/// <see cref="MarkProcessedAsync"/> records success. <see cref="ReleaseClaimAsync"/>
/// drops a claim when the handler fails, so the message can be delivered again.
/// </summary>
public interface IInboxStore
{
    /// <summary>
    /// Returns true if the message with the given ID has already been successfully processed
    /// within the configured deduplication window.
    /// </summary>
    /// <param name="messageId">
    /// Dedup key. Reactor and processor hosted services pass
    /// <c>reactor:{hash}:{message id}</c> or <c>processor:{hash}:{message id}</c>.
    /// <c>hash</c> is a fixed-length hash of the consumer type's full name.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records a message as successfully processed.
    /// Idempotent — calling again for the same ID should not throw.
    /// </summary>
    /// <param name="messageId">
    /// Dedup key. Reactor and processor hosted services pass
    /// <c>reactor:{hash}:{message id}</c> or <c>processor:{hash}:{message id}</c>.
    /// <c>hash</c> is a fixed-length hash of the consumer type's full name.
    /// </param>
    /// <param name="processedAt">When the message was processed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkProcessedAsync(string messageId, DateTimeOffset processedAt, CancellationToken cancellationToken = default);

    /// <summary>
    /// Claims <paramref name="messageId"/> for this caller. False when the key is
    /// already processed inside the deduplication window, or a live claim is held.
    /// A claim lasts for <see cref="MessagingOptions.InboxClaimLease"/> (default 30 seconds)
    /// so a crashed handler can be retried.
    /// </summary>
    /// <param name="messageId">Dedup key. Same shape as <see cref="IsProcessedAsync"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> TryClaimAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drops a claim that has not been marked processed. A processed key is left alone.
    /// </summary>
    /// <param name="messageId">Dedup key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ReleaseClaimAsync(string messageId, CancellationToken cancellationToken = default);
}
