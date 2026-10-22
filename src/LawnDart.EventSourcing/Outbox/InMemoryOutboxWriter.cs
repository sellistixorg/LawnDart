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
    private readonly TimeSpan _claimLease;
    private readonly List<OutboxMessage> _messages = [];
    private readonly Dictionary<Guid, DateTime> _claims = [];
    private readonly object _lock = new();

    /// <summary>Creates a writer. <paramref name="claimLease"/> defaults to 30 seconds.</summary>
    public InMemoryOutboxWriter(TimeSpan? claimLease = null)
    {
        _claimLease = claimLease ?? TimeSpan.FromSeconds(30);
    }

    /// <summary>
    /// Completes at once. This writer keeps messages in memory and has no schema to create.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    /// Run this from a deploy step. Normal app startup should not call it.
    /// This method does not touch a database.
    /// </remarks>
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
            var now = DateTime.UtcNow;
            var unprocessed = _messages
                .Where(m => m.ProcessedAt is null && m.DeadLetteredAt is null)
                .Where(m => !_claims.TryGetValue(m.Id, out var until) || until <= now)
                .OrderBy(m => m.SequencePosition)
                .Take(batchSize)
                .ToList();
            var lease = now + _claimLease;
            foreach (var message in unprocessed)
                _claims[message.Id] = lease;
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
            _claims.Remove(messageId);
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
            _claims.Remove(messageId);
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<bool> ResetDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var message = _messages.FirstOrDefault(m => m.Id == messageId);
            if (message is null || !CanReset(message))
                return Task.FromResult(false);

            message.DeadLetteredAt = null;
            message.Attempts = 0;
            return Task.FromResult(true);
        }
    }

    /// <inheritdoc/>
    public Task<int> ResetAllDeadLetteredAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_lock)
        {
            var count = 0;
            foreach (var message in _messages)
            {
                if (!CanReset(message))
                    continue;

                message.DeadLetteredAt = null;
                message.Attempts = 0;
                count++;
            }

            return Task.FromResult(count);
        }
    }

    private static bool CanReset(OutboxMessage? message)
        => message is not null && message.DeadLetteredAt is not null && message.ProcessedAt is null;

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

            _claims.Remove(messageId);
        }
        return Task.CompletedTask;
    }
}
