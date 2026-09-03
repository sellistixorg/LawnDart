using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight.Registration;

namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Default <see cref="IProjectionRunnerManager"/> for a single bounded context.
/// Holds one <see cref="LightweightProjectionRunnerService"/> per registered projection and
/// supports stop/start with full in-memory state discard (fresh instance on every
/// <see cref="StartAsync"/>).
/// </summary>
public sealed class BoundedContextProjectionRunnerManager : IProjectionRunnerManager
{
    private readonly IReadOnlyDictionary<string, ProjectionRegistration> _byStorageKey;
    private readonly Func<ProjectionRegistration, LightweightProjectionRunnerService> _runnerFactory;
    private readonly ConcurrentDictionary<string, LightweightProjectionRunnerService> _runners
        = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ILogger<BoundedContextProjectionRunnerManager>? _logger;
    private readonly LightweightProjectionSharedPipe? _sharedPipe;

    /// <summary>
    /// Creates a new manager.
    /// </summary>
    /// <param name="registrations">All projection registrations for this context.</param>
    /// <param name="runnerFactory">
    /// Factory that produces a brand-new <see cref="LightweightProjectionRunnerService"/> for the
    /// given registration.  Called on every <see cref="StartAsync"/> to guarantee a fresh
    /// in-memory state.
    /// </param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="eventStore">
    /// When provided with <paramref name="options"/>, enables the process-level shared Subscribe pipe.
    /// </param>
    /// <param name="options">Shared-pipe options; ignored when <paramref name="eventStore"/> is null.</param>
    public BoundedContextProjectionRunnerManager(
        IReadOnlyList<ProjectionRegistration> registrations,
        Func<ProjectionRegistration, LightweightProjectionRunnerService> runnerFactory,
        ILogger<BoundedContextProjectionRunnerManager>? logger = null,
        IEventStore? eventStore = null,
        LightweightProjectionOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(registrations);
        ArgumentNullException.ThrowIfNull(runnerFactory);

        _byStorageKey = registrations.ToDictionary(r => r.StorageKey, StringComparer.Ordinal);
        _runnerFactory = runnerFactory;
        _logger = logger;

        if (eventStore is not null && options is not null)
            _sharedPipe = new LightweightProjectionSharedPipe(eventStore, options, options.ContextName, logger);
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> StorageKeys => [.. _byStorageKey.Keys];

    /// <inheritdoc />
    public async Task StartAsync(string storageKey, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storageKey);

        if (!_byStorageKey.TryGetValue(storageKey, out var reg))
            throw new ArgumentException(
                $"No projection with StorageKey '{storageKey}' is registered in this context. " +
                $"Known keys: {string.Join(", ", _byStorageKey.Keys)}",
                nameof(storageKey));

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // Always create a fresh instance — discards _instances / _dirtyInstances / _lastCheckpointSequence
            if (_runners.TryRemove(storageKey, out var old))
                await SafeStopAndDisposeAsync(old).ConfigureAwait(false);

            var runner = _runnerFactory(reg);
            if (_sharedPipe is { IsEnabled: true })
                runner.AttachSharedPipe(_sharedPipe);
            await runner.StartAsync(ct).ConfigureAwait(false);
            _runners[storageKey] = runner;

            _logger?.LogInformation(
                "[{StorageKey}] Fresh runner started", storageKey);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task StopAsync(string storageKey, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storageKey);

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_runners.TryRemove(storageKey, out var runner))
            {
                await SafeStopAndDisposeAsync(runner).ConfigureAwait(false);
                _logger?.LogInformation("[{StorageKey}] Runner stopped", storageKey);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public bool IsRunning(string storageKey) => _runners.ContainsKey(storageKey);

    /// <inheritdoc />
    public ProjectionRunnerObservabilitySnapshot? GetSnapshot(string storageKey)
        => _runners.TryGetValue(storageKey, out var r) ? r.GetObservabilitySnapshot() : null;

    /// <inheritdoc />
    public bool TryGetView(string storageKey, string instanceId, out string viewJson, out long sequence)
    {
        viewJson = string.Empty;
        sequence = 0;

        if (string.IsNullOrWhiteSpace(storageKey) || string.IsNullOrWhiteSpace(instanceId))
            return false;

        return _runners.TryGetValue(storageKey, out var runner)
               && runner.TryGetView(instanceId, out viewJson, out sequence);
    }

    // ── Boot / shutdown helpers used by the hosted service ───────────────────

    /// <summary>Starts all registered projection runners.  Called at application boot.</summary>
    public async Task StartAllAsync(CancellationToken ct = default)
    {
        var keys = _byStorageKey.Keys.ToList();
        if (_sharedPipe is { IsEnabled: true })
            _sharedPipe.BeginStartBatch(keys.Count);

        foreach (var key in keys)
            await StartAsync(key, ct).ConfigureAwait(false);

        if (_sharedPipe is { IsEnabled: true })
            await _sharedPipe.CompleteStartBatchAsync(keys.Count, ct).ConfigureAwait(false);
    }

    /// <summary>Stops all running projection runners.  Called on application shutdown.</summary>
    public async Task StopAllAsync(CancellationToken ct = default)
    {
        foreach (var key in _runners.Keys.ToList())
            await StopAsync(key, ct).ConfigureAwait(false);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static async Task SafeStopAndDisposeAsync(LightweightProjectionRunnerService runner)
    {
        try { await runner.StopAsync(CancellationToken.None).ConfigureAwait(false); } catch { }
        try { runner.Dispose(); } catch { }
    }
}

/// <summary>
/// Thin <see cref="IHostedService"/> that delegates application start/stop to a
/// <see cref="BoundedContextProjectionRunnerManager"/>.  One instance is registered per
/// bounded context by <c>BoundedContextProjectionExtensions.WithProjections</c>.
/// </summary>
internal sealed class BoundedContextRunnerManagerHostedService : IHostedService
{
    private readonly BoundedContextProjectionRunnerManager _manager;

    public BoundedContextRunnerManagerHostedService(BoundedContextProjectionRunnerManager manager)
    {
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));
    }

    public Task StartAsync(CancellationToken cancellationToken) =>
        _manager.StartAllAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) =>
        _manager.StopAllAsync(cancellationToken);
}
