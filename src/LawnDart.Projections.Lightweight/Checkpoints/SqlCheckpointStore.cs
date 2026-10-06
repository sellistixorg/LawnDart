using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using LawnDart.Projections.Storage;
using LawnDart.Sql;

namespace LawnDart.Projections.Checkpoints;

/// <summary>
/// SQL Server implementation of checkpoint storage.
/// </summary>
public class SqlCheckpointStore : ICheckpointStore
{
    private readonly string _connectionString;
    private readonly ILogger<SqlCheckpointStore>? _logger;
    private readonly string _tableName = "ProjectionCheckpoints";
    private readonly string _schemaName;
    private readonly string _qualifiedTableName;

    internal string QualifiedTableName => _qualifiedTableName;
    private readonly IViewStore _viewStore;

    public SqlCheckpointStore(
        string connectionString, 
        IViewStore viewStore,
        ILogger<SqlCheckpointStore>? logger = null,
        string schemaName = "dbo")
    {
        _connectionString    = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _viewStore           = viewStore ?? throw new ArgumentNullException(nameof(viewStore));
        _logger              = logger;
        _schemaName          = string.IsNullOrWhiteSpace(schemaName) ? "dbo" : schemaName;
        _qualifiedTableName  = SqlIdentifier.Qualify(_schemaName, _tableName);
    }

    public async Task<ProjectionCheckpoint?> GetCheckpointAsync(
        string projectionType,
        int nodeId,
        CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
            SELECT ProjectionType, NodeId, LastSequencePosition, LastUpdated, TotalEventsProcessed
            FROM {_qualifiedTableName}
            WHERE ProjectionType = @ProjectionType AND NodeId = @NodeId";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@ProjectionType", projectionType);
        command.Parameters.AddWithValue("@NodeId", nodeId);

        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return new ProjectionCheckpoint
            {
                ProjectionType = reader.GetString(0),
                NodeId = reader.GetInt32(1),
                LastSequencePosition = reader.GetInt64(2),
                LastUpdated = reader.GetDateTime(3),
                TotalEventsProcessed = reader.GetInt64(4)
            };
        }

        return null;
    }

    public async Task SaveCheckpointAsync(
        ProjectionCheckpoint checkpoint,
        CancellationToken cancellationToken = default)
    {
        const int maxRetries = 3;
        int retryCount = 0;
        
        while (retryCount < maxRetries)
        {
            try
            {
                var connection = new SqlConnection(_connectionString);
                await using var connectionDisposal = connection.ConfigureAwait(false);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                // CRITICAL: LastSequencePosition must be monotonically increasing.
                // Multiple projection instances on the same node process different streams with different sequence positions.
                // We must use MAX to ensure the checkpoint only moves forward, never backward.
                // TotalEventsProcessed is the actual count of events the coordinator read and sent (per-node, per-projection-type).
                // Since there's only one coordinator per projection type per node, we can directly overwrite TotalEventsProcessed.
                var sql = $@"
                    MERGE {_qualifiedTableName} AS target
                    USING (SELECT @ProjectionType AS ProjectionType, @NodeId AS NodeId) AS source
                    ON (target.ProjectionType = source.ProjectionType AND target.NodeId = source.NodeId)
                    WHEN MATCHED THEN
                        UPDATE SET 
                            -- Only update LastSequencePosition if new value is higher (ensures monotonic increase)
                            LastSequencePosition = CASE 
                                WHEN @LastSequencePosition > target.LastSequencePosition 
                                THEN @LastSequencePosition 
                                ELSE target.LastSequencePosition 
                            END,
                            LastUpdated = @LastUpdated,
                            -- TotalEventsProcessed is the actual count from the coordinator (single source of truth)
                            TotalEventsProcessed = @TotalEventsProcessed
                    WHEN NOT MATCHED THEN
                        INSERT (ProjectionType, NodeId, LastSequencePosition, LastUpdated, TotalEventsProcessed)
                        VALUES (@ProjectionType, @NodeId, @LastSequencePosition, @LastUpdated, @TotalEventsProcessed);";

                var command = new SqlCommand(sql, connection);
                await using var commandDisposal = command.ConfigureAwait(false);
                command.Parameters.AddWithValue("@ProjectionType", checkpoint.ProjectionType);
                command.Parameters.AddWithValue("@NodeId", checkpoint.NodeId);
                command.Parameters.AddWithValue("@LastSequencePosition", checkpoint.LastSequencePosition);
                command.Parameters.AddWithValue("@LastUpdated", checkpoint.LastUpdated);
                command.Parameters.AddWithValue("@TotalEventsProcessed", checkpoint.TotalEventsProcessed);

                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                
                if (_logger is not null)
                    SqlCheckpointStoreLog.CheckpointSaved(
                        _logger, checkpoint.ProjectionType, checkpoint.NodeId, checkpoint.LastSequencePosition);
                return; // Success - exit retry loop
            }
            catch (SqlException sqlEx) when (
                (sqlEx.Number == 2 || sqlEx.Number == 10053) && 
                retryCount < maxRetries - 1)
            {
                // 2 = Timeout expired (connection pool exhausted)
                // 10053 = Connection broken/reset
                retryCount++;
                var delay = TimeSpan.FromMilliseconds(50 * Math.Pow(2, retryCount)); // 100ms, 200ms, 400ms
                if (_logger is not null)
                    SqlCheckpointStoreLog.CheckpointSaveRetry(
                        _logger, retryCount, maxRetries, checkpoint.ProjectionType, checkpoint.NodeId, delay.TotalMilliseconds);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (_logger is not null)
                    SqlCheckpointStoreLog.CheckpointSaveFailed(
                        _logger, ex, checkpoint.ProjectionType, checkpoint.NodeId,
                        ex.GetType().FullName, ex.Message, retryCount);
                throw; // Re-throw if not a retryable error or max retries reached
            }
        }
    }

    public async Task<IEnumerable<ProjectionCheckpoint>> GetCheckpointsForProjectionAsync(
        string projectionType,
        CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
            SELECT ProjectionType, NodeId, LastSequencePosition, LastUpdated, TotalEventsProcessed
            FROM {_qualifiedTableName}
            WHERE ProjectionType = @ProjectionType
            ORDER BY NodeId";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@ProjectionType", projectionType);
        var checkpoints = new List<ProjectionCheckpoint>();

        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            checkpoints.Add(new ProjectionCheckpoint
            {
                ProjectionType = reader.GetString(0),
                NodeId = reader.GetInt32(1),
                LastSequencePosition = reader.GetInt64(2),
                LastUpdated = reader.GetDateTime(3),
                TotalEventsProcessed = reader.GetInt64(4)
            });
        }

        return checkpoints;
    }

    public async Task<IEnumerable<ProjectionCheckpoint>> GetAllCheckpointsAsync(
        CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
            SELECT ProjectionType, NodeId, LastSequencePosition, LastUpdated, TotalEventsProcessed
            FROM {_qualifiedTableName}
            ORDER BY ProjectionType, NodeId";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        var checkpoints = new List<ProjectionCheckpoint>();

        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            checkpoints.Add(new ProjectionCheckpoint
            {
                ProjectionType = reader.GetString(0),
                NodeId = reader.GetInt32(1),
                LastSequencePosition = reader.GetInt64(2),
                LastUpdated = reader.GetDateTime(3),
                TotalEventsProcessed = reader.GetInt64(4)
            });
        }

        return checkpoints;
    }

    public async Task<long?> GetStreamCheckpointAsync(
        string projectionType,
        string streamId,
        CancellationToken cancellationToken = default)
    {
        // Delegate to view store to read checkpoint from view metadata
        var result = await _viewStore.GetViewWithCheckpointAsync(
            projectionType, streamId, cancellationToken).ConfigureAwait(false);
        
        if (result.HasValue)
        {
            if (_logger is not null)
                SqlCheckpointStoreLog.StreamCheckpointRetrieved(
                    _logger, projectionType, streamId, result.Value.Checkpoint);
            
            return result.Value.Checkpoint;
        }
        
        return null;
    }

    public Task SaveStreamCheckpointAsync(
        string projectionType,
        string streamId,
        long sequencePosition,
        CancellationToken cancellationToken = default)
    {
        // Checkpoint is saved with view in ProjectionInstanceActor.SaveView()
        // This method is a no-op - checkpoint already persisted with view
        if (_logger is not null)
            SqlCheckpointStoreLog.StreamCheckpointSaveNoOp(_logger, projectionType, streamId, sequencePosition);
        
        return Task.CompletedTask;
    }

    public async Task DeleteCheckpointAsync(
        string projectionType,
        int? nodeId = null,
        CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        string sql;
        if (nodeId.HasValue)
        {
            sql = $"DELETE FROM {_qualifiedTableName} WHERE ProjectionType = @ProjectionType AND NodeId = @NodeId";
        }
        else
        {
            sql = $"DELETE FROM {_qualifiedTableName} WHERE ProjectionType = @ProjectionType";
        }

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@ProjectionType", projectionType);
        if (nodeId.HasValue)
            command.Parameters.AddWithValue("@NodeId", nodeId.Value);

        var rows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _logger?.LogDebug("Deleted {Rows} checkpoint row(s) for {ProjectionType} (nodeId={NodeId})",
            rows, projectionType, nodeId?.ToString() ?? "all");
    }

    /// <summary>
    /// Initializes the checkpoint table schema.
    /// Idempotent - safe to call multiple times.
    /// Migrates existing schema if NodeId column doesn't exist.
    /// </summary>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        // Ensure the target schema exists before creating any objects within it.
        var schemaSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{SqlIdentifier.Literal(_schemaName)}')
            BEGIN
                EXEC('CREATE SCHEMA {SqlIdentifier.Literal(SqlIdentifier.Quote(_schemaName))} AUTHORIZATION dbo');
            END";
        var schemaCmd = new SqlCommand(schemaSql, connection);
        await using var schemaCmdDisposal = schemaCmd.ConfigureAwait(false);
        await schemaCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        // Check if table exists and if it has NodeId column
        var checkColumnSql = $@"
            IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qualifiedTableName)}') AND type in (N'U'))
            BEGIN
                IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qualifiedTableName)}') AND name = 'NodeId')
                BEGIN
                    -- Migrate existing table: Add NodeId column with default 0, then update primary key
                    ALTER TABLE {_qualifiedTableName} ADD [NodeId] INT NOT NULL DEFAULT 0;
                    
                    -- Drop old primary key if it exists
                    DECLARE @PKName NVARCHAR(200) = (SELECT name FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qualifiedTableName)}') AND type = 'PK');
                    IF @PKName IS NOT NULL
                    BEGIN
                        DECLARE @DropPk NVARCHAR(MAX) = N'ALTER TABLE {SqlIdentifier.Literal(_qualifiedTableName)} DROP CONSTRAINT ' + QUOTENAME(@PKName);
                        EXEC(@DropPk);
                    END
                    
                    -- Create new composite primary key
                    ALTER TABLE {_qualifiedTableName} ADD CONSTRAINT {SqlIdentifier.Quote("PK_" + _tableName)} PRIMARY KEY ([ProjectionType], [NodeId]);
                END
            END";

        var createTableSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qualifiedTableName)}') AND type in (N'U'))
            BEGIN
                CREATE TABLE {_qualifiedTableName} (
                    [ProjectionType] NVARCHAR(200) NOT NULL,
                    [NodeId] INT NOT NULL,
                    [LastSequencePosition] BIGINT NOT NULL,
                    [LastUpdated] DATETIME2 NOT NULL,
                    [TotalEventsProcessed] BIGINT NOT NULL DEFAULT 0,
                    CONSTRAINT {SqlIdentifier.Quote("PK_" + _tableName)} PRIMARY KEY ([ProjectionType], [NodeId])
                );
            END";

        // PK (ProjectionType, NodeId) covers all store query patterns.
        // Drop obsolete nonclustered indexes that duplicate the PK or are unused.
        var indexSql = $@"
            IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qualifiedTableName)}') AND type in (N'U'))
            BEGIN
                IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_LastUpdated' AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qualifiedTableName)}'))
                    DROP INDEX [IX_LastUpdated] ON {_qualifiedTableName};

                IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ProjectionType_NodeId' AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qualifiedTableName)}'))
                    DROP INDEX [IX_ProjectionType_NodeId] ON {_qualifiedTableName};
            END";

        try
        {
            // First, migrate existing table if needed
            var migrateCommand = new SqlCommand(checkColumnSql, connection);
            await using var migrateCommandDisposal = migrateCommand.ConfigureAwait(false);
            await migrateCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            
            // Then, create table if it doesn't exist
            var createCommand = new SqlCommand(createTableSql, connection);
            await using var createCommandDisposal = createCommand.ConfigureAwait(false);
            await createCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            var indexCommand = new SqlCommand(indexSql, connection);
            await using var indexCommandDisposal = indexCommand.ConfigureAwait(false);
            await indexCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            
            _logger?.LogInformation(
                "Initialized checkpoint store schema {SchemaName}.{TableName} (with NodeId support)",
                _schemaName, _tableName);
        }
        catch (Microsoft.Data.SqlClient.SqlException ex) when (ex.Number == 2714 || ex.Number == 1913)
        {
            // 2714 = Object already exists, 1913 = Index already exists
            // This can happen in race conditions with multiple nodes initializing simultaneously
            _logger?.LogDebug("Checkpoint store schema already exists (idempotent check): {Message}", ex.Message);
        }
    }
}
