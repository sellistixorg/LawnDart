using LawnDart.EventSourcing.Postgres;
using Npgsql;

namespace LawnDart.EventSourcing.Postgres.Outbox;

/// <summary>
/// Outbox table for one context schema. Format 1. The events stamp is a different row.
/// </summary>
internal static class PostgresOutboxSchema
{
    internal const int CurrentFormat = 1;

    internal const string Component = "outbox";

    /// <summary>
    /// Creates the outbox table when it is missing, then sets autovacuum and the pending index.
    /// A second call with format 1 applies those settings again.
    /// </summary>
    internal static async Task EnsureAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        var qualified = PgIdentifier.Qualify(schema, table);
        var schemaInfo = PgIdentifier.Qualify(schema, "schema_info");
        var index = PgIdentifier.Quote("ix_" + table + "_pending");

        using (var gate = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(hashtextextended(@key, 0))", connection, transaction))
        {
            gate.Parameters.AddWithValue("key", schema + ":outbox");
            await gate.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await ExecuteAsync(connection, transaction, $"CREATE SCHEMA IF NOT EXISTS {PgIdentifier.Quote(schema)}", cancellationToken)
            .ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, $"""
            CREATE TABLE IF NOT EXISTS {schemaInfo} (
                component text PRIMARY KEY,
                format int NOT NULL,
                partition_count int NOT NULL,
                partitioning boolean NOT NULL,
                updated_at timestamptz NOT NULL
            )
            """, cancellationToken).ConfigureAwait(false);

        var exists = await RegclassExistsAsync(connection, transaction, qualified, cancellationToken).ConfigureAwait(false);
        if (exists)
        {
            var found = await ReadFormatAsync(connection, transaction, schemaInfo, cancellationToken).ConfigureAwait(false);
            if (found != CurrentFormat)
                throw new IncompatibleOutboxSchemaException(qualified, found, CurrentFormat);
        }
        else
        {
            await ExecuteAsync(connection, transaction, $"""
                CREATE TABLE {qualified} (
                    id uuid PRIMARY KEY,
                    event_type text NOT NULL,
                    schema_version int NOT NULL,
                    codec_id smallint NOT NULL,
                    payload bytea NOT NULL,
                    metadata jsonb NOT NULL,
                    created_at timestamptz NOT NULL,
                    processed_at timestamptz NULL,
                    attempts int NOT NULL,
                    last_error text NULL,
                    last_attempt_at timestamptz NULL,
                    stream_id text NOT NULL,
                    sequence_position bigint NOT NULL,
                    dead_lettered_at timestamptz NULL,
                    claimed_until timestamptz NULL
                )
                """, cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, transaction, $"""
                INSERT INTO {schemaInfo} (component, format, partition_count, partitioning, updated_at)
                VALUES ('{Component}', {CurrentFormat}, 0, false, clock_timestamp())
                ON CONFLICT (component) DO NOTHING
                """, cancellationToken).ConfigureAwait(false);
            var found = await ReadFormatAsync(connection, transaction, schemaInfo, cancellationToken).ConfigureAwait(false);
            if (found != CurrentFormat)
                throw new IncompatibleOutboxSchemaException(qualified, found, CurrentFormat);
        }

        await ExecuteAsync(connection, transaction, $"""
            CREATE INDEX IF NOT EXISTS {index}
            ON {qualified} (sequence_position)
            WHERE processed_at IS NULL AND dead_lettered_at IS NULL
            """, cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, Autovacuum(qualified), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Autovacuum settings for a table whose rows are inserted, claimed, and deleted.
    /// </summary>
    internal static string Autovacuum(string qualifiedTable)
        => $"""
            ALTER TABLE {qualifiedTable} SET (
                autovacuum_vacuum_scale_factor = 0.01,
                autovacuum_vacuum_threshold = 1000,
                autovacuum_analyze_scale_factor = 0.02,
                autovacuum_analyze_threshold = 1000
            )
            """;

    private static async Task<int?> ReadFormatAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string schemaInfo,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(
            $"SELECT format FROM {schemaInfo} WHERE component = '{Component}'",
            connection,
            transaction);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull ? null : Convert.ToInt32(result);
    }

    private static async Task<bool> RegclassExistsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string qualifiedName,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(
            "SELECT to_regclass(@name)::text",
            connection,
            transaction);
        command.Parameters.AddWithValue("name", qualifiedName);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is string { Length: > 0 };
    }

    private static async Task ExecuteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
