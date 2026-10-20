using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace LawnDart.EventSourcing.Postgres;

/// <summary>
/// Reports <c>pg_notification_queue_usage()</c>, the fraction of the Postgres
/// notification queue that is currently in use.
/// </summary>
/// <remarks>
/// An append that sends a wake-up fails when that queue is full. A listener
/// that has stopped reading is the usual cause. The check is healthy below
/// <see cref="DefaultDegradedUsage"/>, degraded from that mark, and unhealthy
/// from <see cref="DefaultUnhealthyUsage"/>. Pass other marks to the constructor.
/// </remarks>
public sealed class PostgresNotificationQueueHealthCheck : IHealthCheck
{
    /// <summary>Usage at or above this fraction is degraded. Default 0.5.</summary>
    public const double DefaultDegradedUsage = 0.5;

    /// <summary>Usage at or above this fraction is unhealthy. Default 0.8.</summary>
    public const double DefaultUnhealthyUsage = 0.8;

    private readonly string _connectionString;
    private readonly double _degradedAt;
    private readonly double _unhealthyAt;

    /// <summary>
    /// Checks the notification queue on <paramref name="connectionString"/>.
    /// A pooled connection is fine. This check does not listen.
    /// </summary>
    public PostgresNotificationQueueHealthCheck(string connectionString)
        : this(connectionString, DefaultDegradedUsage, DefaultUnhealthyUsage)
    {
    }

    /// <summary>
    /// Checks the notification queue on <paramref name="connectionString"/>
    /// with the given marks. Each mark is a fraction from above 0 through 1.
    /// <paramref name="unhealthyAt"/> must be at least <paramref name="degradedAt"/>.
    /// </summary>
    public PostgresNotificationQueueHealthCheck(string connectionString, double degradedAt, double unhealthyAt)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(connectionString));
        if (degradedAt is <= 0 or > 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(degradedAt),
                degradedAt,
                "The degraded mark must be above 0 and at most 1.");
        }

        if (unhealthyAt is <= 0 or > 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(unhealthyAt),
                unhealthyAt,
                "The unhealthy mark must be above 0 and at most 1.");
        }

        if (unhealthyAt < degradedAt)
            throw new ArgumentException("The unhealthy mark must be at least the degraded mark.");

        _connectionString = connectionString;
        _degradedAt = degradedAt;
        _unhealthyAt = unhealthyAt;
    }

    /// <summary>Reads <c>pg_notification_queue_usage()</c> and maps it to a health result.</summary>
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand("SELECT pg_notification_queue_usage()", connection);
            var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            var usage = result switch
            {
                double number => number,
                float number => number,
                _ => Convert.ToDouble(result)
            };
            return Classify(usage, _degradedAt, _unhealthyAt);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return HealthCheckResult.Unhealthy("The notification queue check did not complete.", ex);
        }
    }

    /// <summary>Maps a queue fraction to healthy, degraded, or unhealthy.</summary>
    internal static HealthCheckResult Classify(double usage, double degradedAt, double unhealthyAt)
    {
        var data = new Dictionary<string, object> { ["notificationQueueUsage"] = usage };
        if (usage >= unhealthyAt)
        {
            return HealthCheckResult.Unhealthy(
                "The Postgres notification queue is past the unhealthy mark. An append that sends a wake-up fails when the queue is full.",
                data: data);
        }

        if (usage >= degradedAt)
        {
            return HealthCheckResult.Degraded(
                "The Postgres notification queue is past the degraded mark. An append that sends a wake-up fails when the queue is full.",
                data: data);
        }

        return HealthCheckResult.Healthy("The Postgres notification queue has room.", data);
    }
}
