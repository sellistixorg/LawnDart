using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace LawnDart.Backends.Contract.Tests;

public sealed class PostgresSubscriptionRulesTests
{
    [Fact]
    public void PoolerHost_MatchesPoolerName_Pgbouncer_AndPort6543()
    {
        Assert.True(PostgresPoolerHost.LooksLikeTransactionPooler(
            "Host=db-pooler.example;Port=5432;Username=u;Password=p;Database=lawndart"));
        Assert.True(PostgresPoolerHost.LooksLikeTransactionPooler(
            "Host=pgbouncer;Port=5432;Username=u;Password=p;Database=lawndart"));
        Assert.True(PostgresPoolerHost.LooksLikeTransactionPooler(
            "Host=127.0.0.1;Port=6543;Username=u;Password=p;Database=lawndart"));
        Assert.False(PostgresPoolerHost.LooksLikeTransactionPooler(
            "Host=localhost;Port=5432;Username=u;Password=p;Database=lawndart"));
    }

    [Fact]
    public void Backoff_GrowsAndThenStaysCapped()
    {
        Assert.Equal(TimeSpan.FromMilliseconds(200), PostgresNotificationListener.BackoffDelay(0));
        Assert.Equal(TimeSpan.FromMilliseconds(400), PostgresNotificationListener.BackoffDelay(1));
        Assert.Equal(TimeSpan.FromMilliseconds(800), PostgresNotificationListener.BackoffDelay(2));
        Assert.Equal(TimeSpan.FromMilliseconds(1600), PostgresNotificationListener.BackoffDelay(3));
        Assert.Equal(TimeSpan.FromMilliseconds(3200), PostgresNotificationListener.BackoffDelay(4));
        Assert.Equal(TimeSpan.FromMilliseconds(5000), PostgresNotificationListener.BackoffDelay(5));
        Assert.Equal(TimeSpan.FromMilliseconds(5000), PostgresNotificationListener.BackoffDelay(9));
    }

    [Fact]
    public void NegativePollInterval_IsRejected()
    {
        var options = new PostgresEventStoreOptions
        {
            ConnectionString = "Host=localhost;Username=u;Password=p;Database=lawndart",
            BackupPollInterval = TimeSpan.FromMilliseconds(-1)
        };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.EnsureValid());
    }

    [Theory]
    [InlineData(0, HealthStatus.Healthy)]
    [InlineData(0.49, HealthStatus.Healthy)]
    [InlineData(0.5, HealthStatus.Degraded)]
    [InlineData(0.79, HealthStatus.Degraded)]
    [InlineData(0.8, HealthStatus.Unhealthy)]
    [InlineData(1, HealthStatus.Unhealthy)]
    public void NotificationQueueMarks_MapUsageToStatus(double usage, HealthStatus status)
    {
        var result = PostgresNotificationQueueHealthCheck.Classify(
            usage,
            PostgresNotificationQueueHealthCheck.DefaultDegradedUsage,
            PostgresNotificationQueueHealthCheck.DefaultUnhealthyUsage);
        Assert.Equal(status, result.Status);
        Assert.Equal(usage, Assert.IsType<double>(result.Data["notificationQueueUsage"]));
    }

    [Fact]
    public void NotificationQueueMarks_RejectAnUnhealthyMarkBelowTheDegradedMark()
    {
        Assert.Throws<ArgumentException>(() =>
            new PostgresNotificationQueueHealthCheck(
                "Host=localhost;Username=u;Password=p;Database=lawndart",
                degradedAt: 0.8,
                unhealthyAt: 0.5));
    }
}
