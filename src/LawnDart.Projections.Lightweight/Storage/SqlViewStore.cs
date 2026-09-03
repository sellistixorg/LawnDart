using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace LawnDart.Projections.Storage;

/// <summary>
/// SQL Server-based view store for persistent storage and admin queries.
/// </summary>
public class SqlViewStore : IViewStore
{
    private readonly string _connectionString;
    private readonly ILogger<SqlViewStore>? _logger;
    private readonly string _tableName = "ProjectionViews";
    private readonly string _schemaName;
    private readonly string _qualifiedTableName;

    /// <summary>
    /// Creates a SQL Server view store.
    /// </summary>
    /// <param name="connectionString">SQL Server connection string.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="schemaName">
    /// SQL schema that owns <c>ProjectionViews</c>. Defaults to <c>dbo</c>.
    /// Whitespace or null is treated as <c>dbo</c>. Use a per-context schema
    /// (commonly the bounded context name) when multiple contexts share one database.
    /// </param>
    public SqlViewStore(
        string connectionString,
        ILogger<SqlViewStore>? logger = null,
        string schemaName = "dbo")
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _logger = logger;
        _schemaName = string.IsNullOrWhiteSpace(schemaName) ? "dbo" : schemaName;
        _qualifiedTableName = $"[{_schemaName}].[{_tableName}]";
    }

    public async Task SaveViewAsync(
        string projectionType,
        string instanceId,
        string viewData,
        long checkpoint,
        CancellationToken cancellationToken = default)
    {
        const int maxRetries = 3;
        int retryCount = 0;
        
        while (retryCount < maxRetries)
        {
            try
            {
                await using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync(cancellationToken);

                // PERFORMANCE FIX: Use UPDATE/INSERT pattern instead of MERGE for better performance
                // MERGE has higher lock overhead and can cause blocking in high-concurrency scenarios
                var sql = $@"
                    UPDATE {_qualifiedTableName}
                    SET ViewData = @ViewData,
                        [Checkpoint] = @Checkpoint,
                        LastUpdated = @LastUpdated
                    WHERE ProjectionType = @ProjectionType 
                      AND InstanceId = @InstanceId;
                    
                    IF @@ROWCOUNT = 0
                    BEGIN
                        INSERT INTO {_qualifiedTableName} (ProjectionType, InstanceId, ViewData, [Checkpoint], LastUpdated)
                        VALUES (@ProjectionType, @InstanceId, @ViewData, @Checkpoint, @LastUpdated);
                    END";

                await using var command = new SqlCommand(sql, connection);
                command.Parameters.AddWithValue("@ProjectionType", projectionType);
                command.Parameters.AddWithValue("@InstanceId", instanceId);
                command.Parameters.AddWithValue("@ViewData", viewData);
                command.Parameters.AddWithValue("@Checkpoint", checkpoint);
                command.Parameters.AddWithValue("@LastUpdated", DateTime.UtcNow);

                await command.ExecuteNonQueryAsync(cancellationToken);
                
                _logger?.LogDebug("Saved view to SQL: {ProjectionType}:{InstanceId} | ViewData size: {Size} bytes | Checkpoint: {Checkpoint}", 
                    projectionType, instanceId, viewData?.Length ?? 0, checkpoint);
                return; // Success - exit retry loop
            }
            catch (SqlException sqlEx) when (
                (sqlEx.Number == 2627 || sqlEx.Number == 2 || sqlEx.Number == 10053) && 
                retryCount < maxRetries - 1)
            {
                // 2627 = Primary key violation
                // 2 = Timeout expired (connection pool exhausted)
                // 10053 = Connection broken/reset
                retryCount++;
                var delay = TimeSpan.FromMilliseconds(50 * Math.Pow(2, retryCount)); // 100ms, 200ms, 400ms
                var errorType = sqlEx.Number == 2627 ? "Primary key violation" : "Connection pool timeout";
                _logger?.LogWarning(
                    "{ErrorType} (retry {RetryCount}/{MaxRetries}) for {ProjectionType}:{InstanceId}, retrying in {Delay}ms",
                    errorType, retryCount, maxRetries, projectionType, instanceId, delay.TotalMilliseconds);
                await Task.Delay(delay, cancellationToken);
            }
            catch (SqlException sqlEx)
            {
                // Log detailed SQL exception with context
                _logger?.LogError(sqlEx,
                    "SQL error saving view: {ProjectionType}:{InstanceId} | " +
                    "SQL Error {Number}: {Message} | " +
                    "State: {State} | Class: {Class} | " +
                    "Procedure: {Procedure} | LineNumber: {LineNumber} | " +
                    "ViewData size: {ViewDataSize} bytes | " +
                    "InstanceId length: {InstanceIdLength} | Connection string database: {ConnectionDatabase} | " +
                    "Retry count: {RetryCount}",
                    projectionType, instanceId,
                    sqlEx.Number, sqlEx.Message, sqlEx.State, sqlEx.Class,
                    sqlEx.Procedure, sqlEx.LineNumber,
                    viewData?.Length ?? 0, instanceId?.Length ?? 0,
                    new SqlConnectionStringBuilder(_connectionString).InitialCatalog,
                    retryCount);
                throw; // Re-throw to be caught by HybridViewStore
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex,
                    "Non-SQL error saving view: {ProjectionType}:{InstanceId} | " +
                    "Error Type: {ErrorType} | Message: {Message} | " +
                    "ViewData size: {ViewDataSize} bytes | InstanceId length: {InstanceIdLength} | " +
                    "Retry count: {RetryCount}",
                    projectionType, instanceId, ex.GetType().FullName, ex.Message,
                    viewData?.Length ?? 0, instanceId?.Length ?? 0, retryCount);
                throw; // Re-throw to be caught by HybridViewStore
            }
        }
    }

    /// <summary>
    /// Bulk upsert via <c>OPENJSON</c> + <c>MERGE</c> in one round-trip per chunk (default chunk 250).
    /// </summary>
    public async Task SaveViewsAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(views);
        if (views.Count == 0)
            return;

        if (views.Count == 1)
        {
            var single = views[0];
            await SaveViewAsync(
                projectionType,
                single.InstanceId,
                single.ViewData,
                single.Checkpoint,
                cancellationToken);
            return;
        }

        // Last write wins for duplicate instance ids in the same batch.
        var deduped = new Dictionary<string, (string ViewData, long Checkpoint)>(views.Count, StringComparer.Ordinal);
        foreach (var (instanceId, viewData, checkpoint) in views)
            deduped[instanceId] = (viewData, checkpoint);

        const int chunkSize = 250;
        var items = deduped.Select(kv => new BulkViewRow(kv.Key, kv.Value.ViewData, kv.Value.Checkpoint)).ToList();

        for (var offset = 0; offset < items.Count; offset += chunkSize)
        {
            var chunk = items.Skip(offset).Take(chunkSize).ToList();
            await SaveViewsChunkAsync(projectionType, chunk, cancellationToken);
        }

        _logger?.LogDebug(
            "Bulk-saved {Count} view(s) to SQL for {ProjectionType}",
            items.Count, projectionType);
    }

    private async Task SaveViewsChunkAsync(
        string projectionType,
        IReadOnlyList<BulkViewRow> chunk,
        CancellationToken cancellationToken)
    {
        const int maxRetries = 3;
        var retryCount = 0;
        var json = JsonSerializer.Serialize(chunk);

        while (retryCount < maxRetries)
        {
            try
            {
                await using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync(cancellationToken);

                // Single round-trip upsert; OPENJSON avoids requiring a persisted TVP type.
                var sql = $@"
                    MERGE {_qualifiedTableName} AS target
                    USING (
                        SELECT
                            @ProjectionType AS ProjectionType,
                            InstanceId,
                            ViewData,
                            [Checkpoint]
                        FROM OPENJSON(@Json)
                        WITH (
                            InstanceId nvarchar(450) '$.InstanceId',
                            ViewData nvarchar(max) '$.ViewData',
                            [Checkpoint] bigint '$.Checkpoint'
                        )
                    ) AS source
                    ON target.ProjectionType = source.ProjectionType
                       AND target.InstanceId = source.InstanceId
                    WHEN MATCHED THEN
                        UPDATE SET
                            ViewData = source.ViewData,
                            [Checkpoint] = source.[Checkpoint],
                            LastUpdated = @LastUpdated
                    WHEN NOT MATCHED THEN
                        INSERT (ProjectionType, InstanceId, ViewData, [Checkpoint], LastUpdated)
                        VALUES (source.ProjectionType, source.InstanceId, source.ViewData, source.[Checkpoint], @LastUpdated);";

                await using var command = new SqlCommand(sql, connection);
                command.Parameters.AddWithValue("@ProjectionType", projectionType);
                command.Parameters.AddWithValue("@Json", json);
                command.Parameters.AddWithValue("@LastUpdated", DateTime.UtcNow);
                command.CommandTimeout = Math.Max(command.CommandTimeout, 120);

                await command.ExecuteNonQueryAsync(cancellationToken);
                return;
            }
            catch (SqlException sqlEx) when (
                (sqlEx.Number == 2627 || sqlEx.Number == 2 || sqlEx.Number == 10053) &&
                retryCount < maxRetries - 1)
            {
                retryCount++;
                var delay = TimeSpan.FromMilliseconds(50 * Math.Pow(2, retryCount));
                _logger?.LogWarning(
                    "Bulk view save retry {RetryCount}/{MaxRetries} for {ProjectionType} ({Count} rows) in {Delay}ms: {Number}",
                    retryCount, maxRetries, projectionType, chunk.Count, delay.TotalMilliseconds, sqlEx.Number);
                await Task.Delay(delay, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex,
                    "Bulk view save failed for {ProjectionType} ({Count} rows) after {RetryCount} retries",
                    projectionType, chunk.Count, retryCount);
                throw;
            }
        }

        throw new InvalidOperationException(
            $"Bulk view save for '{projectionType}' failed after {maxRetries} retries.");
    }

    private sealed record BulkViewRow(string InstanceId, string ViewData, long Checkpoint);

    public async Task<string?> GetViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT ViewData
            FROM {_qualifiedTableName}
            WHERE ProjectionType = @ProjectionType AND InstanceId = @InstanceId";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@ProjectionType", projectionType);
        command.Parameters.AddWithValue("@InstanceId", instanceId);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }

    public async Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(
        string projectionType,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT InstanceId, ViewData
            FROM {_qualifiedTableName}
            WHERE ProjectionType = @ProjectionType
            ORDER BY LastUpdated DESC";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@ProjectionType", projectionType);

        var results = new List<(string, string)>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add((reader.GetString(0), reader.GetString(1)));
        }

        return results;
    }

    public async Task DeleteViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            DELETE FROM {_qualifiedTableName}
            WHERE ProjectionType = @ProjectionType AND InstanceId = @InstanceId";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@ProjectionType", projectionType);
        command.Parameters.AddWithValue("@InstanceId", instanceId);

        await command.ExecuteNonQueryAsync(cancellationToken);
        
        _logger?.LogDebug("Deleted view from SQL: {ProjectionType}:{InstanceId}", projectionType, instanceId);
    }

    public async Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT ViewData, [Checkpoint]
            FROM {_qualifiedTableName}
            WHERE ProjectionType = @ProjectionType AND InstanceId = @InstanceId";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@ProjectionType", projectionType);
        command.Parameters.AddWithValue("@InstanceId", instanceId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            var viewData = reader.GetString(0);
            var checkpoint = reader.GetInt64(1);
            
            _logger?.LogDebug(
                "Retrieved view with checkpoint from SQL: {ProjectionType}:{InstanceId} | Checkpoint: {Checkpoint}",
                projectionType, instanceId, checkpoint);
            
            return (viewData, checkpoint);
        }

        return null;
    }

    public async Task DeleteAllViewsAsync(
        string projectionType,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"DELETE FROM {_qualifiedTableName} WHERE ProjectionType = @ProjectionType";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@ProjectionType", projectionType);

        var rows = await command.ExecuteNonQueryAsync(cancellationToken);
        _logger?.LogDebug("Deleted {Rows} view row(s) for {ProjectionType}", rows, projectionType);
    }

    /// <summary>
    /// Initializes the view store schema and table.
    /// Idempotent — safe to call multiple times. Creates the SQL schema when missing,
    /// creates <c>[{schema}].[ProjectionViews]</c>, and upgrades indexes to match current query patterns.
    /// After objects exist in the target shape, subsequent calls only evaluate catalog existence
    /// checks and skip DDL (suitable for a DML-only login once an elevated init has run).
    /// </summary>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var schemaSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_schemaName}')
            BEGIN
                EXEC('CREATE SCHEMA [{_schemaName}] AUTHORIZATION dbo');
            END";
        await using (var schemaCmd = new SqlCommand(schemaSql, connection))
        {
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var createTableSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{_qualifiedTableName}') AND type in (N'U'))
            BEGIN
                CREATE TABLE {_qualifiedTableName} (
                    [ProjectionType] NVARCHAR(200) NOT NULL,
                    [InstanceId] NVARCHAR(500) NOT NULL,
                    [ViewData] NVARCHAR(MAX) NOT NULL,
                    [Checkpoint] BIGINT NOT NULL,
                    [LastUpdated] DATETIME2 NOT NULL,
                    CONSTRAINT [PK_ProjectionViews] PRIMARY KEY ([ProjectionType], [InstanceId])
                );
            END";

        // Indexes outside the create-table block so existing databases upgrade on re-init.
        // Required: composite for GetViewsByTypeAsync (WHERE ProjectionType ORDER BY LastUpdated DESC).
        // Obsolete: IX_ProjectionType, IX_LastUpdated, IX_Checkpoint (unused or superseded).
        var indexSql = $@"
            IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{_qualifiedTableName}') AND type in (N'U'))
            BEGIN
                IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Checkpoint' AND object_id = OBJECT_ID(N'{_qualifiedTableName}'))
                    DROP INDEX [IX_Checkpoint] ON {_qualifiedTableName};

                IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ProjectionType' AND object_id = OBJECT_ID(N'{_qualifiedTableName}'))
                    DROP INDEX [IX_ProjectionType] ON {_qualifiedTableName};

                IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_LastUpdated' AND object_id = OBJECT_ID(N'{_qualifiedTableName}'))
                    DROP INDEX [IX_LastUpdated] ON {_qualifiedTableName};

                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ProjectionType_LastUpdated' AND object_id = OBJECT_ID(N'{_qualifiedTableName}'))
                    CREATE INDEX [IX_ProjectionType_LastUpdated] ON {_qualifiedTableName} ([ProjectionType], [LastUpdated] DESC);
            END";

        try
        {
            await using (var createCommand = new SqlCommand(createTableSql, connection))
            {
                await createCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var indexCommand = new SqlCommand(indexSql, connection))
            {
                await indexCommand.ExecuteNonQueryAsync(cancellationToken);
            }
            
            _logger?.LogInformation(
                "Initialized view store schema {SchemaName}.{TableName}",
                _schemaName, _tableName);
        }
        catch (SqlException ex) when (ex.Number == 2714 || ex.Number == 1913)
        {
            // 2714 = Object already exists, 1913 = Index already exists
            // This can happen in race conditions with multiple nodes initializing simultaneously
            _logger?.LogDebug("View store schema already exists (idempotent check): {Message}", ex.Message);
        }
    }
}
