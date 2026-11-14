namespace LawnDart.EventSourcing.SqlServer;

/// <summary>
/// Column sizes shared by the SQL Server event store and snapshot tables.
/// </summary>
internal static class SqlServerColumnLimits
{
    /// <summary>
    /// <c>Streams.StreamId</c> is <c>NVARCHAR(255)</c>. Events and snapshots store
    /// <c>StreamKey</c> instead of the text.
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
            $"Stream id is {streamId.Length} characters. SQL Server stores stream ids in NVARCHAR({StreamId}) on Streams. Shorten the id.",
            paramName);
    }
}
