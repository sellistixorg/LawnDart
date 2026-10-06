using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Sql;

namespace LawnDart.Messaging.SqlServer;

/// <summary>
/// SQL Server <see cref="IInboxStore"/>. Dedup rows survive process restarts and are
/// visible to every instance that uses the same table.
/// </summary>
public sealed class SqlServerInboxStore : IInboxStore
{
    /// <summary>Extended property written on a fresh inbox table. Mismatch is a wipe.</summary>
    internal const string SchemaFormatPropertyName = "LawnDart_InboxSchemaFormat";

    /// <summary>Column shape: MessageId NVARCHAR(256) primary key, ProcessedAt DATETIMEOFFSET(7).</summary>
    internal const string CurrentSchemaFormat = "1";

    /// <summary>Maximum characters stored in <c>MessageId</c>.</summary>
    public const int MaxKeyLength = 256;

    private readonly string _connectionString;
    private readonly string _schemaName;
    private readonly string _tableName;
    private readonly string _qualifiedTableName;
    private readonly MessagingOptions _messagingOptions;
    private readonly ILogger<SqlServerInboxStore>? _logger;

    /// <summary>
    /// Creates a store from options. <see cref="MessagingOptions.InboxDeduplicationWindow"/>
    /// decides which rows <see cref="IsProcessedAsync"/> still treats as processed.
    /// </summary>
    /// <param name="options">Connection string, schema, and table.</param>
    /// <param name="messagingOptions">Dedup window.</param>
    /// <param name="logger">Optional logger.</param>
    public SqlServerInboxStore(
        IOptions<SqlServerInboxStoreOptions> options,
        IOptions<MessagingOptions> messagingOptions,
        ILogger<SqlServerInboxStore>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(messagingOptions);

        var inbox = options.Value;
        if (string.IsNullOrWhiteSpace(inbox.ConnectionString))
            throw new ArgumentException("Connection string must not be null or whitespace.", nameof(options));

        _connectionString = inbox.ConnectionString;
        _schemaName = string.IsNullOrWhiteSpace(inbox.SchemaName) ? "dbo" : inbox.SchemaName;
        _tableName = string.IsNullOrWhiteSpace(inbox.TableName) ? "Inbox" : inbox.TableName;
        _qualifiedTableName = SqlIdentifier.Qualify(_schemaName, _tableName);
        _messagingOptions = messagingOptions.Value;
        _logger = logger;
    }

    internal string QualifiedTableName => _qualifiedTableName;

    internal string SchemaNameLiteral => SqlIdentifier.Literal(_schemaName);

    internal string TableNameLiteral => SqlIdentifier.Literal(_tableName);

    /// <inheritdoc />
    public async Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default)
    {
        ValidateKey(messageId);

        var sql = $@"
            SELECT CASE WHEN EXISTS (
                SELECT 1
                FROM {_qualifiedTableName}
                WHERE MessageId = @MessageId AND ProcessedAt >= @Cutoff
            ) THEN 1 ELSE 0 END";

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new SqlCommand(sql, connection);
        AddKey(command, messageId);
        command.Parameters.Add("@Cutoff", SqlDbType.DateTimeOffset).Value = Cutoff();

        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(result) == 1;
    }

    /// <inheritdoc />
    public async Task MarkProcessedAsync(
        string messageId,
        DateTimeOffset processedAt,
        CancellationToken cancellationToken = default)
    {
        ValidateKey(messageId);

        var sql = $@"
            MERGE {_qualifiedTableName} WITH (HOLDLOCK) AS target
            USING (SELECT @MessageId AS MessageId) AS source
            ON target.MessageId = source.MessageId
            WHEN MATCHED THEN
                UPDATE SET ProcessedAt = @ProcessedAt
            WHEN NOT MATCHED THEN
                INSERT (MessageId, ProcessedAt) VALUES (@MessageId, @ProcessedAt);";

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        const int maxAttempts = 5;
        for (var attempt = 1; ; attempt++)
        {
            using var transaction = (SqlTransaction)await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);
            try
            {
                using var command = new SqlCommand(sql, connection, transaction);
                AddKey(command, messageId);
                command.Parameters.Add("@ProcessedAt", SqlDbType.DateTimeOffset).Value = processedAt;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (SqlException ex) when (attempt < maxAttempts && ex.Number is 2627 or 2601 or 1205)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                _logger?.LogDebug(
                    ex,
                    "Retrying inbox mark after SQL error {Number} (attempt {Attempt})",
                    ex.Number,
                    attempt);
            }
        }
    }

    /// <summary>
    /// Creates the schema and inbox table when they are missing.
    /// A second call is a no-op when the table already matches this format.
    /// An existing table with another shape throws <see cref="IncompatibleInboxSchemaException"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        var schemaSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{SqlIdentifier.Literal(_schemaName)}')
            BEGIN
                EXEC('CREATE SCHEMA {SqlIdentifier.Literal(SqlIdentifier.Quote(_schemaName))} AUTHORIZATION dbo');
            END";

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        using (var schemaCmd = new SqlCommand(schemaSql, connection))
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        using (var existsCmd = new SqlCommand(
            $"SELECT CASE WHEN OBJECT_ID(N'{SqlIdentifier.Literal(_qualifiedTableName)}', 'U') IS NULL THEN 0 ELSE 1 END",
            connection))
        {
            var exists = Convert.ToInt32(
                await existsCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) == 1;
            if (exists)
            {
                var found = await ReadSchemaFormatAsync(connection, cancellationToken).ConfigureAwait(false);
                var shapeOk = await HasCurrentShapeAsync(connection, cancellationToken).ConfigureAwait(false);
                if (!string.Equals(found, CurrentSchemaFormat, StringComparison.Ordinal) || !shapeOk)
                    throw new IncompatibleInboxSchemaException(_qualifiedTableName, found, CurrentSchemaFormat, !shapeOk);
                return;
            }
        }

        var pkName = SqlIdentifier.Quote("PK_" + _tableName);
        var indexName = SqlIdentifier.Quote("IX_" + _tableName + "_ProcessedAt");
        var sql = $@"
            CREATE TABLE {_qualifiedTableName} (
                MessageId NVARCHAR(256) NOT NULL,
                ProcessedAt DATETIMEOFFSET(7) NOT NULL,
                CONSTRAINT {pkName} PRIMARY KEY CLUSTERED (MessageId)
            );

            CREATE NONCLUSTERED INDEX {indexName}
            ON {_qualifiedTableName} (ProcessedAt);

            EXEC sys.sp_addextendedproperty
                @name = N'{SchemaFormatPropertyName}',
                @value = N'{CurrentSchemaFormat}',
                @level0type = N'SCHEMA', @level0name = N'{SqlIdentifier.Literal(_schemaName)}',
                @level1type = N'TABLE',  @level1name = N'{SqlIdentifier.Literal(_tableName)}';";

        using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogInformation("Inbox schema initialized for {Table}", _qualifiedTableName);
    }

    /// <summary>
    /// Deletes rows older than <see cref="MessagingOptions.InboxDeduplicationWindow"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows deleted.</returns>
    public async Task<int> PurgeExpiredAsync(CancellationToken cancellationToken = default)
    {
        var sql = $"DELETE FROM {_qualifiedTableName} WHERE ProcessedAt < @Cutoff";

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@Cutoff", SqlDbType.DateTimeOffset).Value = Cutoff();

        var deleted = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogInformation("Purged {Count} expired inbox row(s) from {Table}", deleted, _qualifiedTableName);
        return deleted;
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
                $"Inbox key is {messageId.Length} characters. SQL Server inbox keys must be {MaxKeyLength} characters or fewer.",
                nameof(messageId));
        }
    }

    private static void AddKey(SqlCommand command, string messageId)
        => command.Parameters.Add("@MessageId", SqlDbType.NVarChar, MaxKeyLength).Value = messageId;

    private async Task<string?> ReadSchemaFormatAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT CAST(value AS nvarchar(128))
            FROM sys.extended_properties
            WHERE major_id = OBJECT_ID(@Table)
              AND name = @Name
              AND minor_id = 0
            """;
        using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@Table", SqlDbType.NVarChar, 512).Value = _qualifiedTableName;
        command.Parameters.Add("@Name", SqlDbType.NVarChar, 128).Value = SchemaFormatPropertyName;
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull ? null : Convert.ToString(result);
    }

    private async Task<bool> HasCurrentShapeAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT
                COUNT(*) AS ColumnCount,
                SUM(CASE WHEN c.name = N'MessageId'
                          AND ty.name = N'nvarchar'
                          AND c.max_length = 512
                          AND c.is_nullable = 0
                     THEN 1 ELSE 0 END) AS MessageIdOk,
                SUM(CASE WHEN c.name = N'ProcessedAt'
                          AND ty.name = N'datetimeoffset'
                          AND c.scale = 7
                          AND c.is_nullable = 0
                     THEN 1 ELSE 0 END) AS ProcessedAtOk
            FROM sys.columns c
            INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            WHERE c.object_id = OBJECT_ID(@Table)
            """;

        using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@Table", SqlDbType.NVarChar, 512).Value = _qualifiedTableName;
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return false;

        var columnCount = reader.GetInt32(0);
        var messageIdOk = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
        var processedAtOk = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);
        if (columnCount != 2 || messageIdOk != 1 || processedAtOk != 1)
            return false;

        await reader.CloseAsync().ConfigureAwait(false);

        const string pkSql = """
            SELECT COUNT(*)
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic
                ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            INNER JOIN sys.columns c
                ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID(@Table)
              AND i.is_primary_key = 1
              AND ic.key_ordinal = 1
              AND c.name = N'MessageId'
            """;
        using var pk = new SqlCommand(pkSql, connection);
        pk.Parameters.Add("@Table", SqlDbType.NVarChar, 512).Value = _qualifiedTableName;
        var pkCount = Convert.ToInt32(await pk.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
        return pkCount == 1;
    }
}
