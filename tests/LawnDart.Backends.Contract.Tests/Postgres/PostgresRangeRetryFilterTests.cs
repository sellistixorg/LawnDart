using LawnDart.EventSourcing.Postgres.EventStore;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// The append retries in a new transaction only for "no partition of relation found
/// for row" on the events or event tags table while position ranges are on. That
/// error is raised during the statement, so nothing was written. Every other error,
/// including a check constraint with the same SQL state, is not retried this way.
/// </summary>
public sealed class PostgresRangeRetryFilterTests
{
    private const string Events = "events";
    private const string Tags = "event_tags";

    [Theory]
    [InlineData("events")]
    [InlineData("event_tags")]
    public void NoPartition_OnEventsOrTags_IsRetried(string table)
        => Assert.True(Check(Error("23514", table, constraint: null), ranges: true));

    [Fact]
    public void NoPartition_WithRangesOff_IsNotRetried()
        => Assert.False(Check(Error("23514", Events, constraint: null), ranges: false));

    [Fact]
    public void CheckConstraint_WithTheSameSqlState_IsNotRetried()
        => Assert.False(Check(Error("23514", Events, constraint: "events_version_check"), ranges: true));

    [Theory]
    [InlineData("streams")]
    [InlineData("outbox")]
    [InlineData(null)]
    public void NoPartition_OnAnotherTable_IsNotRetried(string? table)
        => Assert.False(Check(Error("23514", table, constraint: null), ranges: true));

    [Theory]
    [InlineData("23505")]
    [InlineData("40001")]
    [InlineData("P0001")]
    public void OtherSqlStates_AreNotRetriedThisWay(string sqlState)
        => Assert.False(Check(Error(sqlState, Events, constraint: null), ranges: true));

    [Fact]
    public void CustomTableNames_AreMatchedExactly()
    {
        var error = Error("23514", "my_events", constraint: null);
        Assert.True(PostgresEventStore.IsRangeAttachedMeanwhile(error, true, "my_events", "my_tags"));
        Assert.False(PostgresEventStore.IsRangeAttachedMeanwhile(error, true, "My_Events", "my_tags"));
        Assert.False(PostgresEventStore.IsRangeAttachedMeanwhile(error, true, Events, Tags));
    }

    private static bool Check(PostgresException error, bool ranges)
        => PostgresEventStore.IsRangeAttachedMeanwhile(error, ranges, Events, Tags);

    private static PostgresException Error(string sqlState, string? table, string? constraint)
        => new(
            "no partition of relation found for row",
            "ERROR",
            "ERROR",
            sqlState,
            tableName: table,
            constraintName: constraint);
}
