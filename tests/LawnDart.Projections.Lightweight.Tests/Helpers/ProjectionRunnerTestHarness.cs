using System.Text.Json;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Helpers;

internal static class ProjectionRunnerTestHarness
{
    internal static readonly LightweightProjectionOptions AggressivePollOptions = new()
    {
        PollInterval       = TimeSpan.FromMilliseconds(10),
        CheckpointInterval = 1,
        BatchSize          = 1
    };

    internal static EventMetadata MakeMeta() =>
        new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    internal static async Task<LightweightProjectionRunnerService> StartRunnerAsync(
        ProjectionRegistration reg,
        IEventStore eventStore,
        IViewStore views,
        ICheckpointStore checkpoints,
        LightweightProjectionOptions options,
        CancellationToken ct = default)
    {
        _ = ct;
        var runner = new LightweightProjectionRunnerService(
            reg,
            eventStore,
            views,
            checkpoints,
            new SingleNodePartitioningService(),
            options);
        // BackgroundService.StartAsync links its token to ExecuteAsync. A test-wide
        // timeout CTS would stop the runner while WaitForViewAsync is still polling.
        await runner.StartAsync(CancellationToken.None);
        return runner;
    }

    internal static async Task<string?> WaitForViewAsync(
        IViewStore views,
        string storageKey,
        string instanceKey,
        TimeSpan timeout,
        Func<string, bool>? predicate = null)
    {
        using var cts = new CancellationTokenSource(timeout);

        while (!cts.IsCancellationRequested)
        {
            var viewJson = await views.GetViewAsync(storageKey, instanceKey, CancellationToken.None);
            if (viewJson is not null && (predicate is null || predicate(viewJson)))
                return viewJson;

            try
            {
                await Task.Delay(25, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return null;
    }

    internal static CatalogListingView? DeserializeCatalogListing(string json) =>
        JsonSerializer.Deserialize<CatalogListingView>(json, ProjectionViewJson.Read);

    /// <summary>Waits until the global checkpoint reaches at least <paramref name="minPosition"/>.</summary>
    internal static async Task WaitForCheckpointAtLeastAsync(
        ICheckpointStore checkpoints,
        string storageKey,
        long minPosition,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        // Own timeout only — do not link a test-wide timed CTS (it races leftover budget).
        using var cts = new CancellationTokenSource(timeout);

        while (!cts.IsCancellationRequested)
        {
            var checkpoint = await checkpoints.GetCheckpointAsync(storageKey, 0, CancellationToken.None);
            if (checkpoint is not null && checkpoint.LastSequencePosition >= minPosition)
                return;

            try
            {
                await Task.Delay(25, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        var final = await checkpoints.GetCheckpointAsync(storageKey, 0, CancellationToken.None);
        if (final is not null && final.LastSequencePosition >= minPosition)
            return;

        throw new TimeoutException(
            $"Checkpoint for '{storageKey}' did not reach position {minPosition} within {timeout}. " +
            $"Last position: {final?.LastSequencePosition.ToString() ?? "null"}.");
    }

    /// <summary>
    /// Eviction runs after the durable checkpoint write. Wait for the working set to drop
    /// instead of asserting immediately after <see cref="WaitForCheckpointAtLeastAsync"/>.
    /// </summary>
    internal static async Task WaitForWorkingSetAtMostAsync(
        LightweightProjectionRunnerService runner,
        int maxCount,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);

        while (!cts.IsCancellationRequested)
        {
            if (runner.WorkingSetCount <= maxCount)
                return;

            try
            {
                await Task.Delay(25, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        throw new TimeoutException(
            $"Working set {runner.WorkingSetCount} did not drop to <= {maxCount} within {timeout}.");
    }
}
