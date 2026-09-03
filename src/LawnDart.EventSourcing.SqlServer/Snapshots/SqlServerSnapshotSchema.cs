using Microsoft.Data.SqlClient;

namespace LawnDart.EventSourcing.SqlServer.Snapshots;

/// <summary>
/// Idempotent DDL for <c>DcbSnapshots</c> (hashed DCB id) and <c>EventSnapshots</c>
/// (traditional stream id). Shared by <see cref="EventStore.SqlServerEventStore.InitializeSchemaAsync"/>
/// and <see cref="SqlServerSnapshotStore.InitializeSchemaAsync"/>.
/// </summary>
internal static class SqlServerSnapshotSchema
{
    internal const string DefaultDcbTable = "DcbSnapshots";
    internal const string DefaultEventTable = "EventSnapshots";

    internal static string Quote(string identifier)
        => identifier.Replace("]", "]]", StringComparison.Ordinal);

    internal static string Qualify(string schemaName, string tableName)
        => $"[{Quote(schemaName)}].[{Quote(tableName)}]";

    /// <summary>
    /// When the events table is renamed (tests / multi-store in one schema) and the snapshot
    /// table is still at its default, derive a unique name so PK objects do not collide.
    /// </summary>
    internal static string ResolveTableName(string configured, string defaultName, string eventsTable)
    {
        if (string.Equals(configured, defaultName, StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(eventsTable, "Events", StringComparison.OrdinalIgnoreCase))
        {
            return $"{defaultName}_{eventsTable}";
        }

        return configured;
    }

    internal static async Task EnsureCreatedAsync(
        SqlConnection connection,
        string schemaName,
        string dcbTableName,
        string eventTableName,
        CancellationToken cancellationToken)
    {
        var qDcb = Qualify(schemaName, dcbTableName);
        var qEvent = Qualify(schemaName, eventTableName);
        var pkDcb = Quote($"PK_{dcbTableName}");
        var pkEvent = Quote($"PK_{eventTableName}");

        var sql = $@"
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{qDcb}') AND type in (N'U'))
            BEGIN
                CREATE TABLE {qDcb} (
                    [DcbId] CHAR(64) COLLATE Latin1_General_BIN NOT NULL,
                    [GlobalSequence] BIGINT NOT NULL,
                    [TakenAtUtc] DATETIME2 NOT NULL,
                    [StateType] NVARCHAR(512) NOT NULL,
                    [StateData] NVARCHAR(MAX) NOT NULL,
                    [Tags] NVARCHAR(MAX) NULL,
                    [ConsistencyMarker] VARBINARY(512) NULL,
                    [Checksum] CHAR(64) COLLATE Latin1_General_BIN NOT NULL,
                    CONSTRAINT [{pkDcb}] PRIMARY KEY CLUSTERED ([DcbId])
                );
            END

            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{qEvent}') AND type in (N'U'))
            BEGIN
                CREATE TABLE {qEvent} (
                    [StreamId] NVARCHAR(255) NOT NULL,
                    [Version] BIGINT NOT NULL,
                    [GlobalSequence] BIGINT NOT NULL,
                    [TakenAtUtc] DATETIME2 NOT NULL,
                    [StateType] NVARCHAR(512) NOT NULL,
                    [StateData] NVARCHAR(MAX) NOT NULL,
                    [Checksum] CHAR(64) COLLATE Latin1_General_BIN NOT NULL,
                    CONSTRAINT [{pkEvent}] PRIMARY KEY CLUSTERED ([StreamId])
                );
            END";

        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
