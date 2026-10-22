using System.Globalization;
using System.Text.Json;
using LawnDart;
using LawnDart.EventSourcing.Postgres;
using LawnDart.Outbox;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace LawnDart.EventSourcing.Postgres.Outbox;

/// <summary>
/// Postgres outbox writer. Appends that enable the outbox write rows in the append
/// function. This type claims, finishes, and records those rows.
/// </summary>
public sealed class PostgresOutboxWriter : IOutboxWriter
{
    private readonly TimeSpan _claimLease;
    private readonly string _connectionString;
    private readonly string? _migrationConnectionString;
    private readonly string _schemaName;
    private readonly string _tableName;
    private readonly string _qualifiedTableName;
    private readonly ILogger<PostgresOutboxWriter>? _logger;

    /// <summary>
    /// Creates a writer. <paramref name="claimLease"/> defaults to 30 seconds.
    /// <see cref="TimeSpan.Zero"/> lets the next poll claim the row again.
    /// </summary>
    /// <param name="connectionString">Connection for claims and updates.</param>
    /// <param name="tableName">Outbox table. Default <c>outbox</c>.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="schemaName">Schema. Default <c>lawndart</c>. <c>public</c> is rejected.</param>
    /// <param name="claimLease">How long a claim hides the row. Null uses 30 seconds.</param>
    /// <param name="migrationConnectionString">
    /// Optional owner connection for <see cref="InitializeSchemaAsync"/>.
    /// When null, <paramref name="connectionString"/> creates the table.
    /// </param>
    public PostgresOutboxWriter(
        string connectionString,
        string tableName = "outbox",
        ILogger<PostgresOutboxWriter>? logger = null,
        string schemaName = "lawndart",
        TimeSpan? claimLease = null,
        string? migrationConnectionString = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string must not be null or whitespace.", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name must not be null or whitespace.", nameof(tableName));

        _claimLease = claimLease ?? TimeSpan.FromSeconds(30);
        _connectionString = connectionString;
        _migrationConnectionString = migrationConnectionString;
        _schemaName = PgIdentifier.RequireSchema(schemaName);
        _tableName = tableName;
        _qualifiedTableName = PgIdentifier.Qualify(_schemaName, _tableName);
        _logger = logger;
    }

    internal string QualifiedTableName => _qualifiedTableName;

    private string MigrationConnectionString =>
        string.IsNullOrWhiteSpace(_migrationConnectionString)
            ? _connectionString
            : _migrationConnectionString;

    private double ClaimLeaseMilliseconds =>
        _claimLease <= TimeSpan.Zero ? 0 : _claimLease.TotalMilliseconds;

    /// <inheritdoc/>
    public async Task WriteAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await WriteAsync(connection, transaction: null, message, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task WriteBatchAsync(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            foreach (var message in messages)
                await WriteAsync(connection, transaction, message, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Writes one row on an open connection. Appends do not call this. The append function writes those rows.
    /// </summary>
    public async Task WriteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        OutboxMessage message,
        CancellationToken cancellationToken = default)
    {
        var sql = $"""
            INSERT INTO {_qualifiedTableName} (
                id, event_type, schema_version, codec_id, payload, metadata, created_at,
                attempts, stream_id, sequence_position
            )
            VALUES (
                @id, @event_type, @schema_version, @codec_id, @payload, @metadata, @created_at,
                @attempts, @stream_id, @sequence_position
            )
            """;
        using var command = new NpgsqlCommand(sql, connection, transaction);
        AddWriteParameters(command, message);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogDebug("Wrote outbox message {MessageId} for event {EventType}", message.Id, message.EventType);
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<OutboxMessage>> GetUnprocessedAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        // One statement. The inner select locks the chosen rows and skips rows
        // another claim already holds. claimed_until hides them until the lease ends.
        var sql = $"""
            WITH candidate AS (
                SELECT id
                FROM {_qualifiedTableName}
                WHERE processed_at IS NULL
                  AND dead_lettered_at IS NULL
                  AND (claimed_until IS NULL OR claimed_until <= clock_timestamp())
                ORDER BY sequence_position
                LIMIT @batch
                FOR UPDATE SKIP LOCKED
            ),
            updated AS (
                UPDATE {_qualifiedTableName} AS o
                SET claimed_until = clock_timestamp() + (@lease_ms * interval '1 millisecond')
                FROM candidate
                WHERE o.id = candidate.id
                RETURNING
                    o.id, o.event_type, o.schema_version, o.codec_id, o.payload,
                    o.metadata::text AS metadata, o.created_at, o.processed_at, o.attempts,
                    o.last_error, o.last_attempt_at, o.stream_id, o.sequence_position, o.dead_lettered_at
            )
            SELECT * FROM updated ORDER BY sequence_position
            """;

        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("batch", NpgsqlDbType.Integer) { Value = batchSize });
        command.Parameters.Add(new NpgsqlParameter("lease_ms", NpgsqlDbType.Double) { Value = ClaimLeaseMilliseconds });
        return await ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<OutboxMessage>> GetDeadLetteredAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            SELECT
                id, event_type, schema_version, codec_id, payload,
                metadata::text AS metadata, created_at, processed_at, attempts,
                last_error, last_attempt_at, stream_id, sequence_position, dead_lettered_at
            FROM {_qualifiedTableName}
            WHERE dead_lettered_at IS NOT NULL
            ORDER BY sequence_position
            LIMIT @batch
            """;
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("batch", NpgsqlDbType.Integer) { Value = batchSize });
        return await ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE {_qualifiedTableName}
            SET processed_at = clock_timestamp(),
                claimed_until = NULL
            WHERE id = @id
            """;
        await ExecuteAsync(sql, messageId, cancellationToken).ConfigureAwait(false);
        _logger?.LogDebug("Marked outbox message {MessageId} as processed", messageId);
    }

    /// <inheritdoc/>
    public async Task RecordFailureAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE {_qualifiedTableName}
            SET attempts = attempts + 1,
                last_error = @last_error,
                last_attempt_at = clock_timestamp(),
                claimed_until = NULL
            WHERE id = @id
            """;
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("id", NpgsqlDbType.Uuid) { Value = messageId });
        command.Parameters.Add(new NpgsqlParameter("last_error", NpgsqlDbType.Text) { Value = error });
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogWarning("Recorded failure for outbox message {MessageId}: {Error}", messageId, error);
    }

    /// <inheritdoc/>
    public async Task MarkAsDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE {_qualifiedTableName}
            SET dead_lettered_at = clock_timestamp()
            WHERE id = @id AND dead_lettered_at IS NULL
            """;
        await ExecuteAsync(sql, messageId, cancellationToken).ConfigureAwait(false);
        _logger?.LogWarning("Dead-lettered outbox message {MessageId}", messageId);
    }

    /// <inheritdoc/>
    public async Task<bool> ResetDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE {_qualifiedTableName}
            SET dead_lettered_at = NULL,
                attempts = 0
            WHERE id = @id AND dead_lettered_at IS NOT NULL AND processed_at IS NULL
            """;
        var updated = await ExecuteAsync(sql, messageId, cancellationToken).ConfigureAwait(false);
        if (updated > 0)
            _logger?.LogInformation("Reset dead-lettered outbox message {MessageId}", messageId);
        return updated > 0;
    }

    /// <inheritdoc/>
    public async Task<int> ResetAllDeadLetteredAsync(CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE {_qualifiedTableName}
            SET dead_lettered_at = NULL,
                attempts = 0
            WHERE dead_lettered_at IS NOT NULL AND processed_at IS NULL
            """;
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        var updated = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogInformation("Reset {Count} dead-lettered outbox messages", updated);
        return updated;
    }

    /// <summary>
    /// Creates the outbox table when it is missing. A second call keeps format 1.
    /// </summary>
    /// <remarks>
    /// Run this from a deploy step. Normal app startup should not call it.
    /// </remarks>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        using var connection = await OpenMigrationAsync(cancellationToken).ConfigureAwait(false);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await PostgresOutboxSchema.EnsureAsync(
            connection, transaction, _schemaName, _tableName, cancellationToken).ConfigureAwait(false);
        await PostgresRoleGrant.GrantAppRoleAsync(
            connection,
            transaction,
            _schemaName,
            _connectionString,
            MigrationConnectionString,
            cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogInformation("Outbox schema initialized");
    }

    /// <summary>
    /// Reads the outbox format stamp, the outbox table, and the pending index.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether the outbox schema matches, which stamp was found, and any missing objects.</returns>
    /// <remarks>
    /// This method does not create or change objects. It uses the connection passed to the constructor,
    /// not <c>migrationConnectionString</c>.
    /// Call it at startup. <see cref="SchemaCheckResult.ThrowIfMismatch"/> throws when the result does not match.
    /// </remarks>
    public async Task<SchemaCheckResult> VerifySchemaAsync(CancellationToken cancellationToken = default)
    {
        var schemaInfo = PgIdentifier.Qualify(_schemaName, "schema_info");
        var pending = "ix_" + _tableName + "_pending";
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        return await PostgresSchemaProbe.CheckAsync(
            connection,
            PostgresOutboxSchema.CurrentFormat.ToString(CultureInfo.InvariantCulture),
            _schemaName,
            schemaInfo,
            PostgresOutboxSchema.Component,
            [
                new(_tableName, _qualifiedTableName),
                new("schema_info", schemaInfo),
                new(pending, PgIdentifier.Qualify(_schemaName, pending))
            ],
            [],
            [],
            [],
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<int> ExecuteAsync(string sql, Guid messageId, CancellationToken cancellationToken)
    {
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("id", NpgsqlDbType.Uuid) { Value = messageId });
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<NpgsqlConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }

    private async Task<NpgsqlConnection> OpenMigrationAsync(CancellationToken cancellationToken)
    {
        var connection = new NpgsqlConnection(MigrationConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }

    private static void AddWriteParameters(NpgsqlCommand command, OutboxMessage message)
    {
        command.Parameters.Add(new NpgsqlParameter("id", NpgsqlDbType.Uuid) { Value = message.Id });
        command.Parameters.Add(new NpgsqlParameter("event_type", NpgsqlDbType.Text) { Value = message.EventType });
        command.Parameters.Add(new NpgsqlParameter("schema_version", NpgsqlDbType.Integer) { Value = message.SchemaVersion });
        command.Parameters.Add(new NpgsqlParameter("codec_id", NpgsqlDbType.Smallint) { Value = (short)message.CodecId });
        command.Parameters.Add(new NpgsqlParameter("payload", NpgsqlDbType.Bytea)
        {
            Value = message.Payload.IsEmpty ? Array.Empty<byte>() : message.Payload.ToArray()
        });
        command.Parameters.Add(new NpgsqlParameter("metadata", NpgsqlDbType.Jsonb) { Value = MetadataJson(message.Metadata) });
        command.Parameters.Add(new NpgsqlParameter("created_at", NpgsqlDbType.TimestampTz) { Value = AsUtc(message.CreatedAt) });
        command.Parameters.Add(new NpgsqlParameter("attempts", NpgsqlDbType.Integer) { Value = message.Attempts });
        command.Parameters.Add(new NpgsqlParameter("stream_id", NpgsqlDbType.Text) { Value = message.StreamId });
        command.Parameters.Add(new NpgsqlParameter("sequence_position", NpgsqlDbType.Bigint) { Value = message.SequencePosition });
    }

    private static string MetadataJson(string metadata)
    {
        if (string.IsNullOrEmpty(metadata))
            return "{}";

        try
        {
            using var document = JsonDocument.Parse(metadata);
            return metadata;
        }
        catch (JsonException ex)
        {
            throw new ArgumentException("Outbox metadata must be JSON.", ex);
        }
    }

    private static DateTime AsUtc(DateTime value)
        => value.Kind switch
        {
            DateTimeKind.Utc => value,
            DateTimeKind.Local => value.ToUniversalTime(),
            _ => DateTime.SpecifyKind(value, DateTimeKind.Utc)
        };

    private static async Task<IReadOnlyList<OutboxMessage>> ReadAllAsync(
        NpgsqlCommand command,
        CancellationToken cancellationToken)
    {
        var messages = new List<OutboxMessage>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            messages.Add(ReadMessage(reader));
        return messages;
    }

    private static OutboxMessage ReadMessage(NpgsqlDataReader reader)
        => new()
        {
            Id = reader.GetGuid(reader.GetOrdinal("id")),
            EventType = reader.GetString(reader.GetOrdinal("event_type")),
            SchemaVersion = reader.GetInt32(reader.GetOrdinal("schema_version")),
            CodecId = (byte)reader.GetInt16(reader.GetOrdinal("codec_id")),
            Payload = reader.GetFieldValue<byte[]>(reader.GetOrdinal("payload")),
            Metadata = reader.GetString(reader.GetOrdinal("metadata")),
            CreatedAt = reader.GetFieldValue<DateTime>(reader.GetOrdinal("created_at")),
            ProcessedAt = ReadTime(reader, "processed_at"),
            Attempts = reader.GetInt32(reader.GetOrdinal("attempts")),
            LastError = reader.IsDBNull(reader.GetOrdinal("last_error"))
                ? null
                : reader.GetString(reader.GetOrdinal("last_error")),
            LastAttemptAt = ReadTime(reader, "last_attempt_at"),
            StreamId = reader.GetString(reader.GetOrdinal("stream_id")),
            SequencePosition = reader.GetInt64(reader.GetOrdinal("sequence_position")),
            DeadLetteredAt = ReadTime(reader, "dead_lettered_at")
        };

    private static DateTime? ReadTime(NpgsqlDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<DateTime>(ordinal);
    }
}
