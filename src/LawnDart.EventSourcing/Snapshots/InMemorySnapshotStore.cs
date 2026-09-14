using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using LawnDart.Snapshots;

namespace LawnDart.EventSourcing.Snapshots;

/// <summary>
/// Process-local snapshot store for stream aggregates and DCB entities.
/// One entry per key (replace-in-place). Registered by <c>UseInMemory()</c>.
/// </summary>
/// <remarks>
/// Snapshots are a cache of derived state. This store does not use
/// <c>Task.Run</c>; the RDY-05 hosted consumer performs the I/O off the
/// command path. A missing or unreadable snapshot returns empty so the
/// repository falls back to full replay.
/// </remarks>
public sealed class InMemorySnapshotStore : ISnapshotStore, IDcbSnapshotStore, ISnapshotAdmin
{
    private readonly ConcurrentDictionary<string, StreamEntry> _streams = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DcbEntry> _dcb = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public Task<(TState? State, SnapshotInfo? Info)> LoadSnapshotAsync<TState>(
        string streamId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ct.ThrowIfCancellationRequested();

        if (!_streams.TryGetValue(streamId, out var entry))
            return Task.FromResult<(TState?, SnapshotInfo?)>((default, null));

        if (!TryDeserialize<TState>(entry.Payload, out var state) || state is null)
            return Task.FromResult<(TState?, SnapshotInfo?)>((default, null));

        return Task.FromResult<(TState?, SnapshotInfo?)>((state, entry.Info));
    }

    /// <inheritdoc/>
    public Task SaveSnapshotAsync<TState>(
        string streamId, long version, long globalSequence,
        TState state, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(state);
        ct.ThrowIfCancellationRequested();

        var payload = JsonSerializer.SerializeToUtf8Bytes(state, SnapshotJson.Options);
        var info = new SnapshotInfo(version, globalSequence, DateTime.UtcNow);
        _streams[streamId] = new StreamEntry(payload, info);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<(TState? State, SnapshotInfo? Info)> LoadDcbSnapshotAsync<TState>(
        string dcbId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dcbId);
        ct.ThrowIfCancellationRequested();

        if (!_dcb.TryGetValue(dcbId, out var entry))
            return Task.FromResult<(TState?, SnapshotInfo?)>((default, null));

        if (!TryDeserialize<TState>(entry.Payload, out var state) || state is null)
            return Task.FromResult<(TState?, SnapshotInfo?)>((default, null));

        return Task.FromResult<(TState?, SnapshotInfo?)>((state, entry.Info));
    }

    /// <inheritdoc/>
    public Task SaveDcbSnapshotAsync<TState>(
        string dcbId, long globalSequence, TState state,
        byte[]? consistencyMarker = null, IReadOnlyList<string>? loadTags = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dcbId);
        ArgumentNullException.ThrowIfNull(state);
        ct.ThrowIfCancellationRequested();

        _ = consistencyMarker;
        _ = loadTags;
        var payload = JsonSerializer.SerializeToUtf8Bytes(state, SnapshotJson.Options);
        var info = new SnapshotInfo(0, globalSequence, DateTime.UtcNow);
        _dcb[dcbId] = new DcbEntry(payload, info);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task DeleteSnapshotAsync(string streamId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ct.ThrowIfCancellationRequested();
        _streams.TryRemove(streamId, out _);
        _dcb.TryRemove(streamId, out _);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task DeleteAllSnapshotsAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _streams.Clear();
        _dcb.Clear();
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<SnapshotInfo?> GetSnapshotInfoAsync(string streamId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ct.ThrowIfCancellationRequested();

        if (_streams.TryGetValue(streamId, out var stream))
            return Task.FromResult<SnapshotInfo?>(stream.Info);
        if (_dcb.TryGetValue(streamId, out var dcb))
            return Task.FromResult<SnapshotInfo?>(dcb.Info);
        return Task.FromResult<SnapshotInfo?>(null);
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<string> EnumerateSnapshotStreamsAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var id in _streams.Keys)
        {
            ct.ThrowIfCancellationRequested();
            yield return id;
        }

        foreach (var id in _dcb.Keys)
        {
            ct.ThrowIfCancellationRequested();
            yield return id;
        }

        await Task.CompletedTask;
    }

    private static bool TryDeserialize<TState>(byte[] payload, out TState? state)
    {
        try
        {
            state = JsonSerializer.Deserialize<TState>(payload, SnapshotJson.Options);
            return true;
        }
        catch (JsonException)
        {
            state = default;
            return false;
        }
    }

    private sealed record StreamEntry(byte[] Payload, SnapshotInfo Info);
    private sealed record DcbEntry(byte[] Payload, SnapshotInfo Info);
}
