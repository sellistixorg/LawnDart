using Npgsql;

namespace LawnDart.EventSourcing.Postgres.Snapshots;

/// <summary>
/// Idempotent DDL for stream snapshots and dynamic consistency boundary snapshots.
/// </summary>
internal static class PostgresSnapshotSchema
{
    internal const string DefaultEventTable = "event_snapshots";
    internal const string DefaultDcbTable = "dcb_snapshots";

    internal static string EventTable(PostgresEventStoreOptions options)
        => string.IsNullOrWhiteSpace(options.EventSnapshotsTableName)
            ? DefaultEventTable
            : options.EventSnapshotsTableName;

    internal static string DcbTable(PostgresEventStoreOptions options)
        => string.IsNullOrWhiteSpace(options.DcbSnapshotsTableName)
            ? DefaultDcbTable
            : options.DcbSnapshotsTableName;

    internal static async Task EnsureAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        PostgresEventStoreOptions options,
        CancellationToken cancellationToken)
    {
        var schema = PgIdentifier.RequireSchema(options.SchemaName);
        var events = PgIdentifier.Qualify(schema, EventTable(options));
        var dcb = PgIdentifier.Qualify(schema, DcbTable(options));
        var sql = $"""
            CREATE SCHEMA IF NOT EXISTS {PgIdentifier.Quote(schema)};

            CREATE TABLE IF NOT EXISTS {events} (
                tenant_id text COLLATE "C" NOT NULL CHECK (tenant_id <> ''),
                stream_id text COLLATE "C" NOT NULL,
                version bigint NOT NULL,
                global_position bigint NOT NULL,
                taken_at timestamptz NOT NULL,
                state_type text NOT NULL,
                state_data bytea NOT NULL,
                checksum char(64) NOT NULL,
                PRIMARY KEY (tenant_id, stream_id)
            );

            CREATE TABLE IF NOT EXISTS {dcb} (
                dcb_id char(64) NOT NULL,
                global_position bigint NOT NULL,
                taken_at timestamptz NOT NULL,
                state_type text NOT NULL,
                state_data bytea NOT NULL,
                tags text[] NULL,
                consistency_marker bytea NULL,
                checksum char(64) NOT NULL,
                PRIMARY KEY (dcb_id)
            );
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
