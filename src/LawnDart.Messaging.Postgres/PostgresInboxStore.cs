using LawnDart.Messaging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace LawnDart.Messaging.Postgres;

/// <summary>
/// Postgres <see cref="IInboxStore"/>. Dedup rows survive process restarts and are
/// visible to every instance that uses the same table.
/// </summary>
public sealed class PostgresInboxStore : IInboxStore
{
    /// <summary>Format written into <c>schema_info</c> for component <c>inbox</c>.</summary>
    internal const int CurrentSchemaFormat = 1;

    /// <summary>Maximum characters stored in <c>message_id</c>.</summary>
    public const int MaxKeyLength = 256;

    private readonly string _connectionString;
    private readonly string? _migrationConnectionString;
    private readonly string _schemaName;
    private readonly string _tableName;
    private readonly string _qualifiedTableName;
    private readonly MessagingOptions _messagingOptions;
    private readonly ILogger<PostgresInboxStore>? _logger;

    /// <summary>
    /// Creates a store from options. <see cref="MessagingOptions.InboxDeduplicationWindow"/>
    /// decides which rows <see cref="IsProcessedAsync"/> still treats as processed.
    /// </summary>
    /// <param name="options">Connection string, schema, and table.</param>
    /// <param name="messagingOptions">Dedup window and claim lease.</param>
    /// <param name="logger">Optional logger.</param>
    public PostgresInboxStore(
        IOptions<PostgresInboxStoreOptions> options,
        IOptions<MessagingOptions> messagingOptions,
        ILogger<PostgresInboxStore>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(messagingOptions);

        var inbox = options.Value;
        if (string.IsNullOrWhiteSpace(inbox.ConnectionString))
            throw new ArgumentException("Connection string must not be null or whitespace.", nameof(options));
        if (string.IsNullOrWhiteSpace(inbox.TableName))
            throw new ArgumentException("Table name must not be null or whitespace.", nameof(options));

        _connectionString = inbox.ConnectionString;
        _migrationConnectionString = inbox.MigrationConnectionString;
        _schemaName = PgIdentifier.RequireSchema(inbox.SchemaName);
        _tableName = inbox.TableName;
        _qualifiedTableName = PgIdentifier.Qualify(_schemaName, _tableName);
        _messagingOptions = messagingOptions.Value;
        _logger = logger;
    }

    internal string QualifiedTableName => _qualifiedTableName;

    private string MigrationConnectionString =>
        string.IsNullOrWhiteSpace(_migrationConnectionString)
            ? _connectionString
            : _migrationConnectionString;

    /// <inheritdoc />
    public async Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default)
    {
        ValidateKey(messageId);
        var sql = $"""
            SELECT EXISTS (
                SELECT 1
                FROM {_qualifiedTableName}
                WHERE message_id = @message_id AND processed_at >= @cutoff
            )
            """;
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        AddKey(command, messageId);
        command.Parameters.Add(new NpgsqlParameter("cutoff", NpgsqlDbType.TimestampTz) { Value = Cutoff() });
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is true;
    }

    /// <inheritdoc />
    public async Task MarkProcessedAsync(
        string messageId,
        DateTimeOffset processedAt,
        CancellationToken cancellationToken = default)
    {
        ValidateKey(messageId);
        var sql = $"""
            INSERT INTO {_qualifiedTableName} (message_id, processed_at)
            VALUES (@message_id, @processed_at)
            ON CONFLICT (message_id) DO UPDATE
            SET processed_at = EXCLUDED.processed_at,
                claimed_at = NULL
            """;
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        AddKey(command, messageId);
        command.Parameters.Add(new NpgsqlParameter("processed_at", NpgsqlDbType.TimestampTz) { Value = processedAt });
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates the schema and inbox table when they are missing.
    /// A second call is a no-op when the table already matches this format.
    /// An existing table with another format throws <see cref="IncompatibleInboxSchemaException"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    /// Run this from a deploy job, a migration step, or an admin tool, once per deploy.
    /// Normal app startup should not call it. An older copy that restarts and runs it
    /// can change a schema a newer version already upgraded.
    /// </remarks>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        var schema = PgIdentifier.Quote(_schemaName);
        var schemaInfo = PgIdentifier.Qualify(_schemaName, "schema_info");
        var index = PgIdentifier.Quote("ix_" + _tableName + "_processed_at");

        using var connection = await OpenMigrationAsync(cancellationToken).ConfigureAwait(false);
        using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        using (var gate = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(hashtextextended(@key, 0))", connection, transaction))
        {
            gate.Parameters.AddWithValue("key", _schemaName + ":inbox");
            await gate.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await ExecuteAsync(connection, transaction, $"CREATE SCHEMA IF NOT EXISTS {schema}", cancellationToken)
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

        var exists = await RegclassExistsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        if (exists)
        {
            var found = await ReadFormatAsync(connection, transaction, schemaInfo, cancellationToken).ConfigureAwait(false);
            if (found != CurrentSchemaFormat)
                throw new IncompatibleInboxSchemaException(_qualifiedTableName, found, CurrentSchemaFormat);
        }
        else
        {
            await ExecuteAsync(connection, transaction, $"""
                CREATE TABLE {_qualifiedTableName} (
                    message_id text PRIMARY KEY,
                    processed_at timestamptz NULL,
                    claimed_at timestamptz NULL
                )
                """, cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, transaction, $"""
                INSERT INTO {schemaInfo} (component, format, partition_count, partitioning, updated_at)
                VALUES ('inbox', {CurrentSchemaFormat}, 0, false, clock_timestamp())
                ON CONFLICT (component) DO NOTHING
                """, cancellationToken).ConfigureAwait(false);
            var found = await ReadFormatAsync(connection, transaction, schemaInfo, cancellationToken).ConfigureAwait(false);
            if (found != CurrentSchemaFormat)
                throw new IncompatibleInboxSchemaException(_qualifiedTableName, found, CurrentSchemaFormat);
        }

        await ExecuteAsync(connection, transaction, $"""
            CREATE INDEX IF NOT EXISTS {index} ON {_qualifiedTableName} (processed_at)
            """, cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, $"""
            ALTER TABLE {_qualifiedTableName} SET (
                autovacuum_vacuum_scale_factor = 0.01,
                autovacuum_vacuum_threshold = 1000,
                autovacuum_analyze_scale_factor = 0.02,
                autovacuum_analyze_threshold = 1000
            )
            """, cancellationToken).ConfigureAwait(false);
        await GrantAppRoleAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogInformation("Inbox schema initialized for {Table}", _qualifiedTableName);
    }

    /// <summary>
    /// Deletes rows older than <see cref="MessagingOptions.InboxDeduplicationWindow"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows deleted.</returns>
    public async Task<int> PurgeExpiredAsync(CancellationToken cancellationToken = default)
    {
        var sql = $"DELETE FROM {_qualifiedTableName} WHERE processed_at < @cutoff";
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("cutoff", NpgsqlDbType.TimestampTz) { Value = Cutoff() });
        var deleted = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogInformation("Purged {Count} expired inbox row(s) from {Table}", deleted, _qualifiedTableName);
        return deleted;
    }

    /// <inheritdoc />
    public async Task<bool> TryClaimAsync(string messageId, CancellationToken cancellationToken = default)
    {
        ValidateKey(messageId);
        var now = DateTimeOffset.UtcNow;
        var lease = _messagingOptions.InboxClaimLease;
        var leaseCutoff = lease > TimeSpan.Zero ? now - lease : DateTimeOffset.MaxValue;

        // One statement. A transaction pooler can run it. The update runs only when
        // the key is outside the dedup window and has no live claim.
        var sql = $"""
            INSERT INTO {_qualifiedTableName} AS existing (message_id, claimed_at)
            VALUES (@message_id, @now)
            ON CONFLICT (message_id) DO UPDATE
            SET claimed_at = EXCLUDED.claimed_at,
                processed_at = NULL
            WHERE (existing.processed_at IS NULL OR existing.processed_at < @cutoff)
              AND (existing.processed_at IS NOT NULL
                   OR existing.claimed_at IS NULL
                   OR existing.claimed_at < @lease_cutoff)
            RETURNING message_id
            """;

        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        AddKey(command, messageId);
        command.Parameters.Add(new NpgsqlParameter("now", NpgsqlDbType.TimestampTz) { Value = now });
        command.Parameters.Add(new NpgsqlParameter("cutoff", NpgsqlDbType.TimestampTz) { Value = Cutoff() });
        command.Parameters.Add(new NpgsqlParameter("lease_cutoff", NpgsqlDbType.TimestampTz) { Value = leaseCutoff });
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is string;
    }

    /// <inheritdoc />
    public async Task ReleaseClaimAsync(string messageId, CancellationToken cancellationToken = default)
    {
        ValidateKey(messageId);
        var sql = $"""
            DELETE FROM {_qualifiedTableName}
            WHERE message_id = @message_id AND processed_at IS NULL
            """;
        using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new NpgsqlCommand(sql, connection);
        AddKey(command, messageId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private DateTimeOffset Cutoff()
        => DateTimeOffset.UtcNow - _messagingOptions.InboxDeduplicationWindow;

    private static void ValidateKey(string messageId)
    {
        if (string.IsNullOrEmpty(messageId))
            throw new ArgumentException("Inbox key must not be null or empty.", nameof(messageId));
        if (messageId.Length > MaxKeyLength)
        {
            throw new ArgumentException(
                $"Inbox key is {messageId.Length} characters. Postgres inbox keys must be {MaxKeyLength} characters or fewer.",
                nameof(messageId));
        }
    }

    private static void AddKey(NpgsqlCommand command, string messageId)
        => command.Parameters.Add(new NpgsqlParameter("message_id", NpgsqlDbType.Text) { Value = messageId });

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

    private async Task GrantAppRoleAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var app = new NpgsqlConnectionStringBuilder(_connectionString).Username;
        var owner = new NpgsqlConnectionStringBuilder(MigrationConnectionString).Username;
        if (string.IsNullOrWhiteSpace(app) || string.Equals(app, owner, StringComparison.Ordinal))
            return;

        var role = PgIdentifier.Quote(app);
        var schema = PgIdentifier.Quote(_schemaName);
        var sql = $"""
            GRANT USAGE ON SCHEMA {schema} TO {role};
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {role};
            """;
        await ExecuteAsync(connection, transaction, sql, cancellationToken).ConfigureAwait(false);
    }

    private async Task<int?> ReadFormatAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string schemaInfo,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(
            $"SELECT format FROM {schemaInfo} WHERE component = 'inbox'",
            connection,
            transaction);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull ? null : Convert.ToInt32(result);
    }

    private async Task<bool> RegclassExistsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand("SELECT to_regclass(@name)::text", connection, transaction);
        command.Parameters.AddWithValue("name", _qualifiedTableName);
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
