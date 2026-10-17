using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.InMemory;

/// <summary>
/// In-memory inbox deduplication store backed by a <see cref="ConcurrentDictionary{TKey,TValue}"/>.
/// Intended for unit/integration tests and local dev. Not durable across restarts.
/// </summary>
public sealed class InMemoryInboxStore : IInboxStore
{
    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private readonly MessagingOptions _options;

    public InMemoryInboxStore(IOptions<MessagingOptions> options)
    {
        _options = options.Value;
    }

    /// <inheritdoc />
    public Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_entries.TryGetValue(messageId, out var entry) || entry.ProcessedAt is null)
                return Task.FromResult(false);

            if (DateTimeOffset.UtcNow - entry.ProcessedAt.Value > _options.InboxDeduplicationWindow)
            {
                _entries.TryRemove(messageId, out _);
                return Task.FromResult(false);
            }

            return Task.FromResult(true);
        }
    }

    /// <inheritdoc />
    public Task MarkProcessedAsync(string messageId, DateTimeOffset processedAt, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _entries.AddOrUpdate(
                messageId,
                _ => new Entry { ProcessedAt = processedAt },
                (_, existing) =>
                {
                    existing.ProcessedAt = processedAt;
                    existing.ClaimedAt = null;
                    return existing;
                });
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<bool> TryClaimAsync(string messageId, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var now = DateTimeOffset.UtcNow;
            if (_entries.TryGetValue(messageId, out var entry))
            {
                if (entry.ProcessedAt is { } processed
                    && now - processed <= _options.InboxDeduplicationWindow)
                    return Task.FromResult(false);

                if (ClaimIsLive(entry.ClaimedAt, entry.ProcessedAt, now))
                    return Task.FromResult(false);

                entry.ProcessedAt = null;
                entry.ClaimedAt = now;
                return Task.FromResult(true);
            }

            _entries[messageId] = new Entry { ClaimedAt = now };
            return Task.FromResult(true);
        }
    }

    /// <inheritdoc />
    public Task ReleaseClaimAsync(string messageId, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(messageId, out var entry) && entry.ProcessedAt is null)
                _entries.TryRemove(messageId, out _);
        }

        return Task.CompletedTask;
    }

    private bool ClaimIsLive(DateTimeOffset? claimedAt, DateTimeOffset? processedAt, DateTimeOffset now) =>
        _options.InboxClaimLease > TimeSpan.Zero
        && processedAt is null
        && claimedAt is { } claimed
        && now - claimed <= _options.InboxClaimLease;

    private sealed class Entry
    {
        public DateTimeOffset? ProcessedAt { get; set; }
        public DateTimeOffset? ClaimedAt { get; set; }
    }

    /// <summary>
    /// Clears all stored entries. Useful for test teardown.
    /// </summary>
    public void Clear()
    {
        lock (_gate)
            _entries.Clear();
    }

    /// <summary>
    /// Returns the number of currently tracked message IDs.
    /// </summary>
    public int Count
    {
        get
        {
            lock (_gate)
                return _entries.Count;
        }
    }
}
