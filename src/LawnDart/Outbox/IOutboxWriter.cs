namespace LawnDart.Outbox;

/// <summary>
/// Interface for writing outbox messages.
/// Implementations should ensure transactional consistency with event store operations.
/// </summary>
public interface IOutboxWriter
{
    /// <summary>
    /// Writes an outbox message to the outbox table.
    /// This should be done in the same transaction as the event store append.
    /// </summary>
    /// <param name="message">Outbox message to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task WriteAsync(OutboxMessage message, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Writes multiple outbox messages to the outbox table.
    /// This should be done in the same transaction as the event store append.
    /// </summary>
    /// <param name="messages">Outbox messages to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task WriteBatchAsync(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets unprocessed messages from the outbox, ordered by sequence position.
    /// </summary>
    /// <param name="batchSize">Maximum number of messages to return.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of unprocessed outbox messages.</returns>
    Task<IReadOnlyList<OutboxMessage>> GetUnprocessedAsync(int batchSize, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Marks a message as processed.
    /// </summary>
    /// <param name="messageId">ID of the message to mark as processed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Records a failed processing attempt.
    /// </summary>
    /// <param name="messageId">ID of the message.</param>
    /// <param name="error">Error message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task RecordFailureAsync(Guid messageId, string error, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as dead-lettered. The processor will not retry it.
    /// Does not set <see cref="OutboxMessage.ProcessedAt"/>.
    /// </summary>
    /// <param name="messageId">ID of the message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkAsDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets dead-lettered messages, ordered by sequence position.
    /// </summary>
    /// <param name="batchSize">Maximum number of messages to return.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<OutboxMessage>> GetDeadLetteredAsync(int batchSize, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Initializes the outbox schema (creates tables if they don't exist).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task InitializeSchemaAsync(CancellationToken cancellationToken = default);
}
