using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.InMemory;

/// <summary>
/// In-memory inbox deduplication store backed by a <see cref="ConcurrentDictionary{TKey,TValue}"/>.
/// Intended for unit/integration tests and local dev. Not durable across restarts.
/// </summary>
public sealed class InMemoryInboxStore : IInboxStore
{
    private readonly ConcurrentDictionary<string, DateTimeOffset> _processed = new(StringComparer.Ordinal);
    private readonly MessagingOptions _options;

    public InMemoryInboxStore(IOptions<MessagingOptions> options)
    {
        _options = options.Value;
    }

    /// <inheritdoc />
    public Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default)
    {
        if (!_processed.TryGetValue(messageId, out var processedAt))
            return Task.FromResult(false);

        var expired = DateTimeOffset.UtcNow - processedAt > _options.InboxDeduplicationWindow;
        if (expired)
        {
            _processed.TryRemove(messageId, out _);
            return Task.FromResult(false);
        }

        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public Task MarkProcessedAsync(string messageId, DateTimeOffset processedAt, CancellationToken cancellationToken = default)
    {
        _processed[messageId] = processedAt;
        return Task.CompletedTask;
    }

    /// <summary>
    /// Clears all stored entries. Useful for test teardown.
    /// </summary>
    public void Clear() => _processed.Clear();

    /// <summary>
    /// Returns the number of currently tracked message IDs.
    /// </summary>
    public int Count => _processed.Count;
}
