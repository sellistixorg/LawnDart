using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;
using LawnDart.Outbox;

namespace LawnDart.EventSourcing.SqlServer.Outbox;

/// <summary>
/// SQL Server implementation of outbox writer.
/// Uses the same database connection/transaction as the event store for atomic writes.
/// </summary>
public class SqlServerOutboxWriter : IOutboxWriter
{
    private readonly string _connectionString;
    private readonly string _tableName;
    private readonly string _schemaName;
    private readonly ILogger<SqlServerOutboxWriter>? _logger;

    // Pre-computed schema-qualified table name, e.g. [ordering].[Outbox]
    private readonly string _qualifiedTableName;
    
    /// <summary>
    /// Initializes a new instance of the SqlServerOutboxWriter.
    /// </summary>
    public SqlServerOutboxWriter(
        string connectionString,
        string tableName = "Outbox",
        ILogger<SqlServerOutboxWriter>? logger = null,
        string schemaName = "dbo")
    {
        _connectionString      = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _tableName             = tableName;
        _schemaName            = string.IsNullOrWhiteSpace(schemaName) ? "dbo" : schemaName;
        _logger                = logger;
        _qualifiedTableName    = $"[{_schemaName}].[{_tableName}]";
    }
    
    /// <inheritdoc/>
    public async Task WriteAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await WriteAsync(connection, null, message, cancellationToken);
    }
    
    /// <inheritdoc/>
    public async Task WriteBatchAsync(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken = default)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        
        using var transaction = connection.BeginTransaction();
        try
        {
            foreach (var message in messages)
            {
                await WriteAsync(connection, transaction, message, cancellationToken);
            }
            
            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }
    
    /// <summary>
    /// Writes a message using an existing connection and optional transaction.
    /// This allows outbox writes to participate in the event store transaction.
    /// </summary>
    public async Task WriteAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        OutboxMessage message,
        CancellationToken cancellationToken = default)
    {
        var sql = $@"
            INSERT INTO {_qualifiedTableName} (
                Id, EventType, SchemaVersion, ContentType, Payload, Metadata, CreatedAt, 
                Attempts, StreamId, SequencePosition
            )
            VALUES (
                @Id, @EventType, @SchemaVersion, @ContentType, @Payload, @Metadata, @CreatedAt,
                @Attempts, @StreamId, @SequencePosition
            )";
        
        using var command = new SqlCommand(sql, connection, transaction);
        command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = message.Id;
        command.Parameters.Add("@EventType", SqlDbType.NVarChar, 500).Value = message.EventType;
        command.Parameters.Add("@SchemaVersion", SqlDbType.Int).Value =
            EventLogBuffers.NormalizeSchemaVersion(message.SchemaVersion);
        command.Parameters.Add("@ContentType", SqlDbType.NVarChar, 100).Value =
            EventCodec.RequireMime(message.CodecId);
        command.Parameters.Add("@Payload", SqlDbType.NVarChar, -1).Value = message.Payload;
        command.Parameters.Add("@Metadata", SqlDbType.NVarChar, -1).Value = message.Metadata;
        command.Parameters.Add("@CreatedAt", SqlDbType.DateTime2).Value = message.CreatedAt;
        command.Parameters.Add("@Attempts", SqlDbType.Int).Value = message.Attempts;
        command.Parameters.Add("@StreamId", SqlDbType.NVarChar, 500).Value = message.StreamId;
        command.Parameters.Add("@SequencePosition", SqlDbType.BigInt).Value = message.SequencePosition;
        
        await command.ExecuteNonQueryAsync(cancellationToken);
        
        _logger?.LogDebug("Wrote outbox message {MessageId} for event {EventType}", message.Id, message.EventType);
    }
    
    /// <inheritdoc/>
    public async Task<IReadOnlyList<OutboxMessage>> GetUnprocessedAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            SELECT TOP (@BatchSize)
                Id, EventType, Payload, Metadata, CreatedAt, ProcessedAt,
                Attempts, LastError, LastAttemptAt, StreamId, SequencePosition, DeadLetteredAt,
                SchemaVersion, ContentType
            FROM {_qualifiedTableName}
            WHERE ProcessedAt IS NULL AND DeadLetteredAt IS NULL
            ORDER BY SequencePosition ASC";

        return await QueryMessagesAsync(sql, batchSize, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<OutboxMessage>> GetDeadLetteredAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            SELECT TOP (@BatchSize)
                Id, EventType, Payload, Metadata, CreatedAt, ProcessedAt,
                Attempts, LastError, LastAttemptAt, StreamId, SequencePosition, DeadLetteredAt,
                SchemaVersion, ContentType
            FROM {_qualifiedTableName}
            WHERE DeadLetteredAt IS NOT NULL
            ORDER BY SequencePosition ASC";

        return await QueryMessagesAsync(sql, batchSize, cancellationToken);
    }

    private async Task<IReadOnlyList<OutboxMessage>> QueryMessagesAsync(
        string sql, int batchSize, CancellationToken cancellationToken)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;

        var messages = new List<OutboxMessage>();

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            messages.Add(ReadMessage(reader));
        }

        return messages;
    }

    private static OutboxMessage ReadMessage(SqlDataReader reader)
    {
        var schemaOrdinal = reader.GetOrdinal("SchemaVersion");
        var contentOrdinal = reader.GetOrdinal("ContentType");
        var schemaVersion = reader.IsDBNull(schemaOrdinal)
            ? 1
            : EventLogBuffers.NormalizeSchemaVersion(reader.GetInt32(schemaOrdinal));
        var contentType = reader.IsDBNull(contentOrdinal)
            ? EventCodec.JsonMime
            : reader.GetString(contentOrdinal);
        if (string.IsNullOrWhiteSpace(contentType))
            contentType = EventCodec.JsonMime;

        return new()
        {
            Id = reader.GetGuid(reader.GetOrdinal("Id")),
            EventType = reader.GetString(reader.GetOrdinal("EventType")),
            SchemaVersion = schemaVersion,
            CodecId = EventCodec.IdFor(contentType),
            Payload = reader.GetString(reader.GetOrdinal("Payload")),
            Metadata = reader.GetString(reader.GetOrdinal("Metadata")),
            CreatedAt = reader.GetDateTime(reader.GetOrdinal("CreatedAt")),
            ProcessedAt = reader.IsDBNull(reader.GetOrdinal("ProcessedAt"))
                ? null
                : reader.GetDateTime(reader.GetOrdinal("ProcessedAt")),
            Attempts = reader.GetInt32(reader.GetOrdinal("Attempts")),
            LastError = reader.IsDBNull(reader.GetOrdinal("LastError"))
                ? null
                : reader.GetString(reader.GetOrdinal("LastError")),
            LastAttemptAt = reader.IsDBNull(reader.GetOrdinal("LastAttemptAt"))
                ? null
                : reader.GetDateTime(reader.GetOrdinal("LastAttemptAt")),
            StreamId = reader.GetString(reader.GetOrdinal("StreamId")),
            SequencePosition = reader.GetInt64(reader.GetOrdinal("SequencePosition")),
            DeadLetteredAt = reader.IsDBNull(reader.GetOrdinal("DeadLetteredAt"))
                ? null
                : reader.GetDateTime(reader.GetOrdinal("DeadLetteredAt"))
        };
    }
    
    /// <inheritdoc/>
    public async Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            UPDATE {_qualifiedTableName}
            SET ProcessedAt = @ProcessedAt
            WHERE Id = @Id";
        
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        
        using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@ProcessedAt", SqlDbType.DateTime2).Value = DateTime.UtcNow;
        command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = messageId;
        
        await command.ExecuteNonQueryAsync(cancellationToken);
        
        _logger?.LogDebug("Marked outbox message {MessageId} as processed", messageId);
    }
    
    /// <inheritdoc/>
    public async Task RecordFailureAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            UPDATE {_qualifiedTableName}
            SET Attempts = Attempts + 1,
                LastError = @LastError,
                LastAttemptAt = @LastAttemptAt
            WHERE Id = @Id";
        
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        
        using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@LastError", SqlDbType.NVarChar, -1).Value = error;
        command.Parameters.Add("@LastAttemptAt", SqlDbType.DateTime2).Value = DateTime.UtcNow;
        command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = messageId;
        
        await command.ExecuteNonQueryAsync(cancellationToken);
        
        _logger?.LogWarning("Recorded failure for outbox message {MessageId}: {Error}", messageId, error);
    }

    /// <inheritdoc/>
    public async Task MarkAsDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            UPDATE {_qualifiedTableName}
            SET DeadLetteredAt = @DeadLetteredAt
            WHERE Id = @Id AND DeadLetteredAt IS NULL";

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@DeadLetteredAt", SqlDbType.DateTime2).Value = DateTime.UtcNow;
        command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = messageId;

        await command.ExecuteNonQueryAsync(cancellationToken);

        _logger?.LogWarning("Dead-lettered outbox message {MessageId}", messageId);
    }
    
    /// <summary>
    /// Initializes the outbox table schema.
    /// </summary>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        var sql = $@"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_schemaName}')
            BEGIN
                EXEC('CREATE SCHEMA [{_schemaName}] AUTHORIZATION dbo');
            END

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{_tableName}' AND schema_id = SCHEMA_ID('{_schemaName}'))
            BEGIN
                CREATE TABLE {_qualifiedTableName} (
                    Id UNIQUEIDENTIFIER PRIMARY KEY,
                    EventType NVARCHAR(500) NOT NULL,
                    SchemaVersion INT NOT NULL DEFAULT 1,
                    ContentType NVARCHAR(100) NOT NULL DEFAULT 'application/json',
                    Payload NVARCHAR(MAX) NOT NULL,
                    Metadata NVARCHAR(MAX) NOT NULL,
                    CreatedAt DATETIME2 NOT NULL,
                    ProcessedAt DATETIME2 NULL,
                    Attempts INT NOT NULL DEFAULT 0,
                    LastError NVARCHAR(MAX) NULL,
                    LastAttemptAt DATETIME2 NULL,
                    StreamId NVARCHAR(500) NOT NULL,
                    SequencePosition BIGINT NOT NULL,
                    DeadLetteredAt DATETIME2 NULL
                );
                
                CREATE INDEX IX_{_tableName}_ProcessedAt_SequencePosition 
                ON {_qualifiedTableName}(ProcessedAt, SequencePosition)
                WHERE ProcessedAt IS NULL;
                
                CREATE INDEX IX_{_tableName}_CreatedAt 
                ON {_qualifiedTableName}(CreatedAt);
            END

            IF EXISTS (SELECT * FROM sys.tables WHERE name = '{_tableName}' AND schema_id = SCHEMA_ID('{_schemaName}'))
            AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'{_qualifiedTableName}') AND name = 'DeadLetteredAt')
            BEGIN
                ALTER TABLE {_qualifiedTableName} ADD DeadLetteredAt DATETIME2 NULL;
            END

            IF EXISTS (SELECT * FROM sys.tables WHERE name = '{_tableName}' AND schema_id = SCHEMA_ID('{_schemaName}'))
            AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'{_qualifiedTableName}') AND name = 'SchemaVersion')
            BEGIN
                ALTER TABLE {_qualifiedTableName} ADD SchemaVersion INT NOT NULL
                    CONSTRAINT [DF_{_tableName}_SchemaVersion] DEFAULT 1;
            END

            IF EXISTS (SELECT * FROM sys.tables WHERE name = '{_tableName}' AND schema_id = SCHEMA_ID('{_schemaName}'))
            AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'{_qualifiedTableName}') AND name = 'ContentType')
            BEGIN
                ALTER TABLE {_qualifiedTableName} ADD ContentType NVARCHAR(100) NOT NULL
                    CONSTRAINT [DF_{_tableName}_ContentType] DEFAULT 'application/json';
            END";
        
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        
        using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
        
        _logger?.LogInformation("Outbox schema initialized");
    }
}
