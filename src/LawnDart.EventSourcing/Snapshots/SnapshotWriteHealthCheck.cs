using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace LawnDart.EventSourcing.Snapshots;

/// <summary>
/// Degraded when snapshot writes have been dropped or have failed.
/// The event log remains the source of truth.
/// </summary>
public sealed class SnapshotWriteHealthCheck : IHealthCheck
{
    private readonly ISnapshotWriteQueue _queue;

    internal SnapshotWriteHealthCheck(ISnapshotWriteQueue queue)
    {
        _queue = queue;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (!_queue.IsDegraded)
            return Task.FromResult(HealthCheckResult.Healthy());

        var data = new Dictionary<string, object>
        {
            ["dropped"] = _queue.DroppedCount,
            ["failures"] = _queue.FailureCount
        };

        return Task.FromResult(HealthCheckResult.Degraded(
            "Snapshot write channel has drops or store failures. The event log is still authoritative; fallback is full replay.",
            data: data));
    }
}
