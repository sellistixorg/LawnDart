namespace LawnDart.EventSourcing.SqlServer;

/// <summary>
/// Column sizes shared by the SQL Server event store and snapshot tables.
/// </summary>
internal static class SqlServerColumnLimits
{
    /// <summary>
    /// <c>Events.StreamId</c>, <c>Streams.StreamId</c>, and <c>EventSnapshots.StreamId</c>
    /// are <c>NVARCHAR(255)</c>.
    /// </summary>
    internal const int StreamId = 255;

    /// <summary>
    /// Throws <see cref="ArgumentException"/> when <paramref name="streamId"/> is longer
    /// than <see cref="StreamId"/>.
    /// </summary>
    internal static void EnsureStreamId(string? streamId, string paramName)
    {
        if (streamId is null || streamId.Length <= StreamId)
            return;

        throw new ArgumentException(
            $"Stream id is {streamId.Length} characters. SQL Server stores stream ids in NVARCHAR({StreamId}) on Events, Streams, and EventSnapshots. Shorten the id.",
            paramName);
    }
}
