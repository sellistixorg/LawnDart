using LawnDart.Outbox;

namespace LawnDart.EventSourcing.Outbox;

/// <summary>
/// Process-local <see cref="IOutboxWriter"/>. Registered by <c>UseInMemory()</c>.
/// </summary>
/// <remarks>
/// Messages live in memory for the lifetime of the writer. There is no schema
/// to initialize. Concurrent calls are serialized with a lock.
/// </remarks>
public sealed class InMemoryOutboxWriter : IOutboxWriter
{
    private readonly List<OutboxMessage> _messages = [];
    private readonly object _lock = new();

    /// <inheritdoc/>
    public Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    /// <inheritdoc/>
    public Task WriteAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            _messages.Add(message);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task WriteBatchAsync(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
            _messages.AddRange(messages);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<OutboxMessage>> GetUnprocessedAsync(
        int batchSize, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var unprocessed = _messages
                .Where(m => m.ProcessedAt is null && m.DeadLetteredAt is null)
                .OrderBy(m => m.SequencePosition)
                .Take(batchSize)
                .ToList();
            return Task.FromResult<IReadOnlyList<OutboxMessage>>(unprocessed);
        }
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<OutboxMessage>> GetDeadLetteredAsync(
        int batchSize, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var dead = _messages
                .Where(m => m.DeadLetteredAt is not null)
                .OrderBy(m => m.SequencePosition)
                .Take(batchSize)
                .ToList();
            return Task.FromResult<IReadOnlyList<OutboxMessage>>(dead);
        }
    }

    /// <inheritdoc/>
    public Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var message = _messages.FirstOrDefault(m => m.Id == messageId);
            if (message is not null)
                message.ProcessedAt = DateTime.UtcNow;
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task MarkAsDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var message = _messages.FirstOrDefault(m => m.Id == messageId);
            if (message is not null && message.DeadLetteredAt is null)
                message.DeadLetteredAt = DateTime.UtcNow;
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task RecordFailureAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var message = _messages.FirstOrDefault(m => m.Id == messageId);
            if (message is not null)
            {
                message.Attempts++;
                message.LastError = error;
                message.LastAttemptAt = DateTime.UtcNow;
            }
        }
        return Task.CompletedTask;
    }
}
