using System.Collections.Concurrent;
using System.Data;
using System.Runtime.CompilerServices;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;
using LawnDart.Serialization;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventSourcing.SqlServer.Snapshots;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// SQL Server implementation of the event store.
/// Implements portable <see cref="IEventStoreSubscriptions"/> with poll-backed live delivery.
/// </summary>
public class SqlServerEventStore : IEventStore, IEventStoreSubscriptions
{
    private readonly IEventSerializer _serializer;
    private readonly string _connectionString;
    private readonly string _tableName;
    private readonly string _registryTableName;
    private readonly string _tagsTableName;
    private readonly bool _enableRegistry;
    private readonly bool _useEventTagsTable;
    private readonly IOutboxWriter? _outboxWriter;
    private readonly SqlServerEventStoreOptions _options;
    private readonly ILogger<SqlServerEventStore>? _logger;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ConcurrentDictionary<SqlServerSubscriptionHandle, byte> _subscriptions = new();
    private bool _hasLoggedConnection = false;

    /// <summary>The bounded context name this store belongs to.</summary>
    public string ContextName { get; }

    // Schema-qualification helpers — pre-computed once in the constructor.
    private readonly string _schemaName;
    private readonly string _qTable;
    private readonly string _qRegistry;
    private readonly string _qTags;
    private readonly string _qSequence;

    public SqlServerEventStore(
        string connectionString,
        IEventSerializer? serializer = null,
        string tableName = "Events",
        string registryTableName = "Streams",
        bool enableRegistry = true,
        SqlServerEventStoreOptions? options = null,
        IOutboxWriter? outboxWriter = null,
        ILogger<SqlServerEventStore>? logger = null)
    {
        _serializer = serializer ?? new JsonEventSerializer();
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _registryTableName = registryTableName ?? throw new ArgumentNullException(nameof(registryTableName));
        _enableRegistry = enableRegistry;
        _options = options ?? new SqlServerEventStoreOptions();
        // If caller uses a custom events table but leaves tags table at default, derive a per-table
        // tags table name to avoid cross-table FK coupling in tests or multi-tenant schemas.
        _tagsTableName =
            string.Equals(_options.EventTagsTableName, "EventTags", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(_tableName, "Events", StringComparison.OrdinalIgnoreCase)
                ? $"EventTags_{_tableName}"
                : _options.EventTagsTableName;
        _useEventTagsTable = _options.UseEventTagsTable;
        _outboxWriter = outboxWriter;
        _logger = logger;

        // Pre-compute schema-qualified names (SchemaName defaults to "dbo" for backward compat)
        _schemaName  = string.IsNullOrWhiteSpace(_options.SchemaName) ? "dbo" : _options.SchemaName;
        ContextName  = string.IsNullOrWhiteSpace(_options.ContextName) ? "default" : _options.ContextName;
        _qTable      = $"[{_schemaName}].[{_tableName}]";
        _qRegistry  = $"[{_schemaName}].[{_registryTableName}]";
        _qTags      = $"[{_schemaName}].[{_tagsTableName}]";
        _qSequence  = $"[{_schemaName}].[EventSequencePosition]";

        // Optimize connection string for performance
        // NOTE: If connection string already has pool settings, preserve them
        // Otherwise, set reasonable defaults
        var builder = new SqlConnectionStringBuilder(connectionString ?? throw new ArgumentNullException(nameof(connectionString)));
        
        // Only override if not already set (allows Program.cs to configure pool size)
        if (builder.MaxPoolSize <= 0 || builder.MaxPoolSize < 100)
        {
            builder.MaxPoolSize = 500;  // Default if not set, but Program.cs should set higher
        }
        if (builder.MinPoolSize <= 0)
        {
            builder.MinPoolSize = 20;  // Pre-warm connections
        }
        if (builder.ConnectTimeout <= 0)
        {
            builder.ConnectTimeout = 60;  // Longer timeout for pool exhaustion
        }
        builder.Pooling = true;  // Ensure pooling enabled
        builder.MultipleActiveResultSets = false;  // Not needed, improves performance
        
        _connectionString = builder.ConnectionString;
        
        // Optimize JSON serialization options
        // Note: Using default PascalCase naming (not camelCase) because positional records require
        // JSON property names to match constructor parameter names exactly. camelCase would serialize
        // as "productId" but constructor expects "ProductId", causing deserialization to fail.
        _jsonOptions = new JsonSerializerOptions
        {
            // PropertyNamingPolicy = JsonNamingPolicy.CamelCase,  // Removed - incompatible with positional records
            PropertyNameCaseInsensitive = true,  // Still useful for other types
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };
    }

    public async Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT StreamId, Version, SequencePosition, EventType, EventData, ContentType, Tags, Metadata, Timestamp
            FROM {_qTable}
            WHERE StreamId = @StreamId AND Version >= @FromVersion";

        if (toVersion.HasValue)
            sql += " AND Version <= @ToVersion";
        if (toTimestamp.HasValue)
            sql += " AND Timestamp <= @ToTimestamp";

        sql += " ORDER BY Version";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@StreamId", streamId);
        command.Parameters.AddWithValue("@FromVersion", fromVersion);
        if (toVersion.HasValue)
            command.Parameters.AddWithValue("@ToVersion", toVersion.Value);
        if (toTimestamp.HasValue)
            command.Parameters.AddWithValue("@ToTimestamp", toTimestamp.Value);

        var events = new List<SequencedEvent>();

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var sequencedEvent = ReadSequencedEvent(reader);
            events.Add(sequencedEvent);
        }

        _logger?.LogDebug(
            "Read {Count} events from stream {StreamId} starting from version {FromVersion}",
            events.Count,
            streamId,
            fromVersion);

        return events.AsReadOnly();
    }

    public async IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT StreamId, Version, SequencePosition, EventType, EventData, ContentType, Tags, Metadata, Timestamp
            FROM {_qTable}
            WHERE StreamId = @StreamId AND Version >= @FromVersion";

        if (toVersion.HasValue)
            sql += " AND Version <= @ToVersion";
        if (toTimestamp.HasValue)
            sql += " AND Timestamp <= @ToTimestamp";

        sql += " ORDER BY Version";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@StreamId", streamId);
        command.Parameters.AddWithValue("@FromVersion", fromVersion);
        if (toVersion.HasValue)
            command.Parameters.AddWithValue("@ToVersion", toVersion.Value);
        if (toTimestamp.HasValue)
            command.Parameters.AddWithValue("@ToTimestamp", toTimestamp.Value);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return ReadSequencedEvent(reader);
        }
    }

    public async Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        if (query == null)
            throw new ArgumentNullException(nameof(query));

        var built = DcbQuerySql.Build(
            _qTable,
            _qTags,
            _useEventTagsTable,
            query,
            fromSequencePosition,
            toSequencePosition,
            toTimestamp,
            limit,
            DcbQuerySql.Mode.Events);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(built.Sql, connection);
        command.Parameters.AddRange(built.Parameters.ToArray());

        var events = new List<SequencedEvent>();

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            events.Add(ReadSequencedEvent(reader));
        }

        _logger?.LogDebug(
            "Read {Count} events by query from sequence position {FromPosition}",
            events.Count,
            fromSequencePosition);

        // ConsistencyMarker is null for SQL Server; only hash-based backends populate it.
        return new QueryResult(events.AsReadOnly());
    }

    public async IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (query == null)
            throw new ArgumentNullException(nameof(query));

        var built = DcbQuerySql.Build(
            _qTable,
            _qTags,
            _useEventTagsTable,
            query,
            fromSequencePosition,
            toSequencePosition,
            toTimestamp,
            limit: null,
            DcbQuerySql.Mode.Events);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(built.Sql, connection);
        command.Parameters.AddRange(built.Parameters.ToArray());

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return ReadSequencedEvent(reader);
        }
    }

    public async Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        var eventsList = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (eventsList.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));

        // Validate tenant ID matches between stream ID and metadata (behavior depends on RequireTenantId)
        var tenantIdFromStream = ExtractTenantId(streamId);
        var tenantIdFromMetadata = metadata?.TenantId;

        if (_options.RequireTenantId && tenantIdFromStream == null)
        {
            throw new ArgumentException(
                $"Stream ID '{streamId}' must include tenant ID in format '{{tenantId}}:{{aggregateType}}:{{aggregateId}}' " +
                $"when RequireTenantId is true.",
                nameof(streamId));
        }

        // If stream includes tenant and metadata includes tenant, they must match
        if (tenantIdFromStream != null && tenantIdFromMetadata != null && tenantIdFromMetadata != tenantIdFromStream)
        {
            throw new ArgumentException(
                $"Tenant ID mismatch: stream ID has '{tenantIdFromStream}' but metadata has '{tenantIdFromMetadata}'",
                nameof(metadata));
        }

        var tagsList = tags?.ToList() ?? new List<string>();

        // Log connection details for debugging (first write only, to avoid spam)
        if (_logger != null && !_hasLoggedConnection)
        {
            var builder = new SqlConnectionStringBuilder(_connectionString);
            _logger.LogInformation("EventStore connecting to: {DataSource}, Database: {Database}", 
                builder.DataSource, builder.InitialCatalog);
            _hasLoggedConnection = true;
        }

        return await ExecuteWithRetryAsync(async ct =>
        {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var transaction = connection.BeginTransaction();

        try
        {
            // Check expected version for optimistic concurrency
            if (expectedVersion.HasValue)
            {
                var currentVersion = await GetCurrentVersionAsync(connection, transaction, streamId, ct);
                if (currentVersion != expectedVersion.Value)
                {
                    // Don't rollback here - let the catch block handle it
                    throw new ConcurrencyException(
                        $"Expected version {expectedVersion.Value} but current version is {currentVersion}",
                        expectedVersion.Value,
                        currentVersion);
                }
            }

            // Get starting version
            // Match InMemoryEventStore behavior: versions are 1-based (first event is version 1)
            var streamCurrentVersion = await GetCurrentVersionAsync(connection, transaction, streamId, ct);
            // If stream doesn't exist (currentVersion = -1), start at version 1
            // Otherwise, start at currentVersion + 1
            var startingVersion = streamCurrentVersion < 0 ? 1 : streamCurrentVersion + 1;

            // Optimize: Use batch insert for better performance (especially for multiple events)
            var sequencePositions = await InsertEventsBatchAsync(
                connection,
                transaction,
                streamId,
                startingVersion,
                eventsList,
                metadata,
                tagsList,
                ct);

            // Update stream registry
            if (_enableRegistry)
            {
                await UpsertStreamMetadataAsync(
                    connection,
                    transaction,
                    streamId,
                    startingVersion + eventsList.Count - 1,
                    sequencePositions.Last(),
                    eventsList.Count,
                    tagsList,
                    ct);
            }

            // Write to outbox if enabled (in same transaction for atomicity)
            if (_options.EnableOutbox && _outboxWriter != null)
            {
                await WriteEventsToOutboxAsync(
                    connection,
                    transaction,
                    eventsList,
                    sequencePositions.ToList(),
                    streamId,
                    metadata,
                    tagsList,
                    ct);
            }

            await transaction.CommitAsync(ct);

            _logger?.LogDebug(
                "Appended {Count} events to stream {StreamId}, versions {FromVersion}-{ToVersion}",
                eventsList.Count,
                streamId,
                startingVersion,
                startingVersion + eventsList.Count - 1);

            return new AppendResult(sequencePositions, null, startingVersion + eventsList.Count - 1);
        }
        catch
        {
            // Only rollback if transaction is still active
            if (transaction.Connection != null)
            {
                try
                {
                    await transaction.RollbackAsync(ct);
                }
                catch
                {
                    // Transaction may already be rolled back, ignore
                }
            }
            throw;
        }
        }, cancellationToken);
    }

    public async Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        var eventsList = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (eventsList.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));

        if (condition == null)
            throw new ArgumentNullException(nameof(condition));

        var tagsList = tags?.ToList() ?? new List<string>();

        return await ExecuteWithRetryAsync(async ct =>
        {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var transaction = connection.BeginTransaction();

        try
        {
            // Range-lock EventTags (sorted) before the existence check so concurrent
            // FailIfMatches on the same tag cannot both pass under READ COMMITTED.
            await AcquireDcbFenceLocksAsync(connection, transaction, condition, ct);

            if (await AppendConditionMatchesAsync(connection, transaction, condition, ct))
            {
                await transaction.RollbackAsync(ct);
                throw new ConcurrencyException(
                    "Append condition failed: matching events exist",
                    condition.After);
            }

            // For DCB approach, stream ID is an internal implementation detail.
            // We generate a unique stream ID, optionally tenant-scoped when RequireTenantId is enabled.
            var tenantId = metadata?.TenantId;
            if (_options.RequireTenantId && string.IsNullOrWhiteSpace(tenantId))
            {
                throw new ArgumentException(
                    "TenantId is required in event metadata when RequireTenantId is true for DCB appends.",
                    nameof(metadata));
            }

            var streamId = !string.IsNullOrWhiteSpace(tenantId)
                ? $"{tenantId}:dcb:{Guid.NewGuid()}"
                : $"dcb:{Guid.NewGuid()}";

            // Optimize: Use batch insert for DCB approach too
            var sequencePositions = await InsertEventsBatchAsync(
                connection,
                transaction,
                streamId,
                0, // Version not meaningful for DCB
                eventsList,
                metadata,
                tagsList,
                ct);

            // Update stream registry for DCB streams
            if (_enableRegistry)
            {
                await UpsertStreamMetadataAsync(
                    connection,
                    transaction,
                    streamId,
                    0, // Version not meaningful for DCB
                    sequencePositions.Last(),
                    eventsList.Count,
                    tagsList,
                    ct);
            }

            // Write to outbox if enabled (in same transaction for atomicity)
            if (_options.EnableOutbox && _outboxWriter != null)
            {
                await WriteEventsToOutboxAsync(
                    connection,
                    transaction,
                    eventsList,
                    sequencePositions.ToList(),
                    streamId,
                    metadata,
                    tagsList,
                    ct);
            }

            await transaction.CommitAsync(ct);

            _logger?.LogDebug(
                "Appended {Count} events with DCB condition, sequence positions {FromPosition}-{ToPosition}",
                eventsList.Count,
                sequencePositions.FirstOrDefault(),
                sequencePositions.LastOrDefault());

            // ConsistencyMarker is null for SQL Server; only hash-based backends populate it.
            return new AppendResult(sequencePositions);
        }
        catch
        {
            // Only rollback if transaction is still active
            if (transaction.Connection != null)
            {
                try
                {
                    await transaction.RollbackAsync(ct);
                }
                catch
                {
                    // Transaction may already be rolled back, ignore
                }
            }
            throw;
        }
        }, cancellationToken);
    }

    private async Task<long> GetCurrentVersionAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        CancellationToken cancellationToken)
    {
        // Optimize: Use registry if available (much faster than MAX query)
        if (_enableRegistry)
        {
            var metadata = await GetStreamMetadataAsync(connection, transaction, streamId, cancellationToken);
            if (metadata != null)
            {
                return metadata.CurrentVersion;
            }
        }
        
        // Fallback to MAX query with covering index
        var sql = $@"
            SELECT TOP 1 Version
            FROM {_qTable} WITH (READPAST)
            WHERE StreamId = @StreamId
            ORDER BY Version DESC";

        await using var command = new SqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("@StreamId", streamId);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result != null && result != DBNull.Value ? Convert.ToInt64(result) : -1;
    }

    private async Task<long> GetNextSequencePositionAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        // Use SEQUENCE object for better performance (no table lock)
        var sql = $"SELECT NEXT VALUE FOR {_qSequence}";

        await using var command = new SqlCommand(sql, connection, transaction);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result);
    }

    private async Task<IReadOnlyList<long>> InsertEventsBatchAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        long startingVersion,
        IReadOnlyList<IEvent> events,
        EventMetadata? metadata,
        IReadOnlyList<string> tags,
        CancellationToken cancellationToken)
    {
        // For single event, use simple insert
        if (events.Count == 1)
        {
            var @event = events[0];
            var eventMetadata = metadata ?? new EventMetadata
            {
                EventId = @event.Id.ToString(),
                Timestamp = @event.Timestamp
            };
            // Set CommitTimestamp to actual commit time
            eventMetadata.CommitTimestamp = DateTime.UtcNow;
            
            var sequencePosition = await InsertEventAsync(
                connection,
                transaction,
                streamId,
                startingVersion,
                @event,
                eventMetadata,
                tags,
                cancellationToken);
            
            return new[] { sequencePosition };
        }

        // SQL Server has a limit of 2100 parameters per query
        // With 8 parameters per event, we can do ~200 events per batch (1600 parameters)
        const int maxEventsPerBatch = 200;
        
        var allSequencePositions = new List<long>();
        var tagsJson = JsonSerializer.Serialize(tags, _jsonOptions);
        
        // Process events in chunks to avoid parameter limit
        // Generate sequence positions per chunk to avoid 1000+ sequential DB calls
        for (int chunkStart = 0; chunkStart < events.Count; chunkStart += maxEventsPerBatch)
        {
            var chunkEnd = Math.Min(chunkStart + maxEventsPerBatch, events.Count);
            var chunkSize = chunkEnd - chunkStart;
            
            // Generate sequence positions for this chunk only
            var sequencePositions = new List<long>();
            for (int i = 0; i < chunkSize; i++)
            {
                var seqPos = await GetNextSequencePositionAsync(connection, transaction, cancellationToken);
                sequencePositions.Add(seqPos);
            }
            
            var values = new List<string>();
            var parameters = new List<SqlParameter>();
            
            for (int i = 0; i < chunkSize; i++)
            {
                var eventIndex = chunkStart + i;
                var @event = events[eventIndex];
                var version = startingVersion + eventIndex;
                var eventMetadata = metadata ?? new EventMetadata
                {
                    EventId = @event.Id.ToString(),
                    Timestamp = @event.Timestamp
                };
                // Set CommitTimestamp to actual commit time
                eventMetadata.CommitTimestamp = DateTime.UtcNow;
                
                var eventTypeName = EventTypeNameResolver.GetName(@event.GetType());
                // Serialize using pluggable serializer
                var eventDataSerialized = _serializer.Serialize(@event, @event.GetType());
                var metadataJson = JsonSerializer.Serialize(eventMetadata, _jsonOptions);
                
                var streamIdParam = $"@StreamId{eventIndex}";
                var versionParam = $"@Version{eventIndex}";
                var seqPosParam = $"@SequencePosition{eventIndex}";
                var eventTypeParam = $"@EventType{eventIndex}";
                var eventDataParam = $"@EventData{eventIndex}";
                var contentTypeParam = $"@ContentType{eventIndex}";
                var tagsParam = $"@Tags{eventIndex}";
                var metadataParam = $"@Metadata{eventIndex}";
                var timestampParam = $"@Timestamp{eventIndex}";
                var partitionHashParam = $"@PartitionHash{eventIndex}";
                
                values.Add($"({streamIdParam}, {versionParam}, {seqPosParam}, {eventTypeParam}, {eventDataParam}, {contentTypeParam}, {tagsParam}, {metadataParam}, {timestampParam}, {partitionHashParam})");
                
                parameters.Add(new SqlParameter(streamIdParam, streamId));
                parameters.Add(new SqlParameter(versionParam, version));
                parameters.Add(new SqlParameter(seqPosParam, sequencePositions[i]));
                parameters.Add(new SqlParameter(eventTypeParam, eventTypeName));
                parameters.Add(new SqlParameter(eventDataParam, eventDataSerialized));
                parameters.Add(new SqlParameter(contentTypeParam, _serializer.ContentType));
                parameters.Add(new SqlParameter(tagsParam, tagsJson));
                parameters.Add(new SqlParameter(metadataParam, metadataJson));
                parameters.Add(new SqlParameter(timestampParam, eventMetadata.Timestamp));
                parameters.Add(new SqlParameter(partitionHashParam, PartitionHashUtility.GetDeterministicHashCode(streamId)));
            }
            
            // Batch insert chunk with explicit sequence positions
            var sql = $@"
                INSERT INTO {_qTable}
                (StreamId, Version, SequencePosition, EventType, EventData, ContentType, Tags, Metadata, Timestamp, PartitionHash)
                VALUES {string.Join(", ", values)}";

            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddRange(parameters.ToArray());
            await command.ExecuteNonQueryAsync(cancellationToken);

            // Populate normalized EventTags table in the same transaction for indexed lookups
            if (_useEventTagsTable && tags.Count > 0)
            {
                var tagValues = new List<string>();
                var tagParameters = new List<SqlParameter>();
                int tagIdx = 0;
                foreach (var seqPos in sequencePositions)
                {
                    foreach (var tag in tags)
                    {
                        tagValues.Add($"(@SeqPos{tagIdx}, @TagVal{tagIdx})");
                        tagParameters.Add(new SqlParameter($"@SeqPos{tagIdx}", seqPos));
                        tagParameters.Add(new SqlParameter($"@TagVal{tagIdx}", tag));
                        tagIdx++;
                    }
                }
                var tagSql = $@"
                    INSERT INTO {_qTags} (GlobalSequencePosition, Tag)
                    VALUES {string.Join(", ", tagValues)}";
                await using var tagCmd = new SqlCommand(tagSql, connection, transaction);
                tagCmd.Parameters.AddRange(tagParameters.ToArray());
                await tagCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            // Add sequence positions for this chunk
            allSequencePositions.AddRange(sequencePositions);
        }

        return allSequencePositions.AsReadOnly();
    }

    private async Task<long> InsertEventAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        long version,
        IEvent @event,
        EventMetadata metadata,
        IReadOnlyList<string> tags,
        CancellationToken cancellationToken)
    {
        // Get sequence position from SEQUENCE
        var sequencePosition = await GetNextSequencePositionAsync(connection, transaction, cancellationToken);
        
        var sql = $@"
            INSERT INTO {_qTable}
            (StreamId, Version, SequencePosition, EventType, EventData, ContentType, Tags, Metadata, Timestamp, PartitionHash)
            VALUES
            (@StreamId, @Version, @SequencePosition, @EventType, @EventData, @ContentType, @Tags, @Metadata, @Timestamp, @PartitionHash)";

        await using var command = new SqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("@StreamId", streamId);
        command.Parameters.AddWithValue("@Version", version);
        command.Parameters.AddWithValue("@SequencePosition", sequencePosition);
        // Store the catalog token from [EventTypeName]. CLR FullName is never written.
        var eventTypeName = EventTypeNameResolver.GetName(@event.GetType());
        command.Parameters.AddWithValue("@EventType", eventTypeName);
        // Serialize using the pluggable serializer
        command.Parameters.AddWithValue("@EventData", _serializer.Serialize(@event, @event.GetType()));
        command.Parameters.AddWithValue("@ContentType", _serializer.ContentType);
        command.Parameters.AddWithValue("@Tags", JsonSerializer.Serialize(tags, _jsonOptions));
        command.Parameters.AddWithValue("@Metadata", JsonSerializer.Serialize(metadata, _jsonOptions));
        command.Parameters.AddWithValue("@Timestamp", metadata.Timestamp);
        command.Parameters.AddWithValue("@PartitionHash", PartitionHashUtility.GetDeterministicHashCode(streamId));

        await command.ExecuteNonQueryAsync(cancellationToken);

        // Populate normalized EventTags table in the same transaction
        if (_useEventTagsTable && tags.Count > 0)
        {
            var tagValues = new List<string>();
            var tagCmd = new SqlCommand("", connection, transaction);
            for (int i = 0; i < tags.Count; i++)
            {
                tagValues.Add($"(@SeqPos{i}, @TagVal{i})");
                tagCmd.Parameters.AddWithValue($"@SeqPos{i}", sequencePosition);
                tagCmd.Parameters.AddWithValue($"@TagVal{i}", tags[i]);
            }
            tagCmd.CommandText = $@"
                INSERT INTO {_qTags} (GlobalSequencePosition, Tag)
                VALUES {string.Join(", ", tagValues)}";
            await using (tagCmd)
                await tagCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        return sequencePosition;
    }

    private SequencedEvent ReadSequencedEvent(SqlDataReader reader)
    {
        var streamId = reader.GetString("StreamId");
        var version = reader.GetInt64("Version");
        var sequencePosition = reader.GetInt64("SequencePosition");
        var eventType = reader.GetString("EventType");
        var eventDataSerialized = reader.GetString("EventData");
        
        // Read ContentType (default to JSON for backward compatibility)
        var contentType = reader.IsDBNull(reader.GetOrdinal("ContentType")) 
            ? "application/json" 
            : reader.GetString("ContentType");
        
        var tagsJson = reader.IsDBNull(reader.GetOrdinal("Tags")) ? "[]" : reader.GetString("Tags");
        var metadataJson = reader.IsDBNull(reader.GetOrdinal("Metadata")) ? "{}" : reader.GetString("Metadata");
        var timestamp = reader.GetDateTime("Timestamp");

        // Deserialize event - resolve type (handles nested types)
        var eventTypeObj = ResolveEventType(eventType);
        if (eventTypeObj == null)
        {
            throw new InvalidOperationException($"Cannot resolve event type: {eventType}");
        }

        // Validate serializer matches content type
        if (_serializer.ContentType != contentType)
        {
            throw new InvalidOperationException(
                $"Event stored with ContentType '{contentType}' but current serializer uses '{_serializer.ContentType}'. " +
                "Use the migration tool to convert streams or configure the correct serializer.");
        }

        // Deserialize event using pluggable serializer
        var @event = (IEvent)_serializer.Deserialize(eventDataSerialized, eventTypeObj);
        var tags = JsonSerializer.Deserialize<string[]>(tagsJson, _jsonOptions) ?? Array.Empty<string>();
        var metadata = JsonSerializer.Deserialize<EventMetadata>(metadataJson, _jsonOptions) ?? new EventMetadata();

        return new SequencedEvent(
            @event,
            sequencePosition,
            streamId,
            version,
            metadata,
            tags);
    }

    /// <summary>
    /// Takes <c>UPDLOCK, HOLDLOCK</c> on each condition tag (sorted) so a concurrent
    /// insert of the same tag cannot sneak in between the existence check and commit.
    /// When <see cref="SqlServerEventStoreOptions.UseEventTagsTable"/> is false, the
    /// existence query itself holds the equivalent locks on <c>Events</c>.
    /// </summary>
    private async Task AcquireDcbFenceLocksAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        AppendCondition condition,
        CancellationToken cancellationToken)
    {
        if (!_useEventTagsTable)
            return;

        var tags = CollectFenceTags(condition);
        if (tags.Count == 0)
        {
            var eventsSql = $@"
                SELECT TOP 1 1
                FROM {_qTable} WITH (UPDLOCK, HOLDLOCK, ROWLOCK)
                WHERE 1 = 1";
            var eventsParams = new List<SqlParameter>();
            if (condition.After.HasValue)
            {
                eventsSql += " AND SequencePosition > @After";
                eventsParams.Add(new SqlParameter("@After", condition.After.Value));
            }

            await using var eventsCmd = new SqlCommand(eventsSql, connection, transaction);
            eventsCmd.Parameters.AddRange(eventsParams.ToArray());
            await eventsCmd.ExecuteScalarAsync(cancellationToken);
            return;
        }

        foreach (var tag in tags)
        {
            var sql = $@"
                SELECT TOP 1 1
                FROM {_qTags} WITH (UPDLOCK, HOLDLOCK, ROWLOCK)
                WHERE Tag = @Tag";
            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.Add(new SqlParameter("@Tag", tag));
            if (condition.After.HasValue)
            {
                command.CommandText += " AND GlobalSequencePosition > @After";
                command.Parameters.Add(new SqlParameter("@After", condition.After.Value));
            }

            await command.ExecuteScalarAsync(cancellationToken);
        }
    }

    private static List<string> CollectFenceTags(AppendCondition condition)
    {
        var tags = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in condition.FailIfEventsMatch.Items)
        {
            if (item.Tags is not { Count: > 0 })
                continue;
            foreach (var tag in item.Tags)
            {
                if (!string.IsNullOrEmpty(tag))
                    tags.Add(tag);
            }
        }

        var sorted = tags.ToList();
        sorted.Sort(StringComparer.Ordinal);
        return sorted;
    }

    /// <summary>
    /// Existence-only condition check: no event payload deserialize.
    /// </summary>
    private async Task<bool> AppendConditionMatchesAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        AppendCondition condition,
        CancellationToken cancellationToken)
    {
        var hints = _useEventTagsTable ? string.Empty : " WITH (UPDLOCK, HOLDLOCK, ROWLOCK)";
        var sql = $@"
            SELECT TOP 1 1
            FROM {_qTable} e{hints}
            WHERE 1=1";

        var parameters = new List<SqlParameter>();

        if (condition.After.HasValue)
        {
            sql += " AND e.SequencePosition > @After";
            parameters.Add(new SqlParameter("@After", condition.After.Value));
        }

        if (condition.FailIfEventsMatch.Items.Count > 0)
        {
            var conditions = new List<string>();
            for (int i = 0; i < condition.FailIfEventsMatch.Items.Count; i++)
            {
                var item = condition.FailIfEventsMatch.Items[i];
                var conditionParts = new List<string>();

                if (item.Types != null && item.Types.Count > 0)
                {
                    var typeConditions = new List<string>();
                    for (int j = 0; j < item.Types.Count; j++)
                    {
                        var typeName = item.Types[j];
                        var paramName = $"@Type{i}_{j}";
                        var paramNamePrefix = $"@TypePrefix{i}_{j}";

                        typeConditions.Add($"(e.EventType = {paramName} OR e.EventType LIKE {paramNamePrefix})");
                        parameters.Add(new SqlParameter(paramName, typeName));
                        parameters.Add(new SqlParameter(paramNamePrefix, typeName + ",%"));
                    }
                    conditionParts.Add($"({string.Join(" OR ", typeConditions)})");
                }

                if (item.Tags != null && item.Tags.Count > 0)
                {
                    for (int j = 0; j < item.Tags.Count; j++)
                    {
                        var tag = item.Tags[j];
                        var paramName = $"@Tag{i}_{j}";
                        if (_useEventTagsTable)
                        {
                            conditionParts.Add($"EXISTS (SELECT 1 FROM {_qTags} et WHERE et.GlobalSequencePosition = e.SequencePosition AND et.Tag = {paramName})");
                        }
                        else
                        {
                            conditionParts.Add($"EXISTS (SELECT 1 FROM OPENJSON(e.Tags) WHERE value = {paramName})");
                        }
                        parameters.Add(new SqlParameter(paramName, tag));
                    }
                }

                if (conditionParts.Count > 0)
                {
                    conditions.Add($"({string.Join(" AND ", conditionParts)})");
                }
            }

            if (conditions.Count > 0)
            {
                sql += $" AND ({string.Join(" OR ", conditions)})";
            }
        }

        await using var command = new SqlCommand(sql, connection, transaction);
        command.Parameters.AddRange(parameters.ToArray());
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is not null && result is not DBNull;
    }

    private static string? DetermineStreamIdFromTags(IReadOnlyList<string> tags)
    {
        foreach (var tag in tags)
        {
            if (tag.Contains(':'))
            {
                return tag;
            }
        }

        return null;
    }

    /// <summary>
    /// Executes a database operation with linear back-off retry for transient SQL failures
    /// (deadlocks, connection failures, and command timeouts).
    /// The retry budget is controlled by <see cref="SqlServerEventStoreOptions.MaxRetryCount"/>
    /// and <see cref="SqlServerEventStoreOptions.RetryDelay"/>.
    /// </summary>
    private async Task<T> ExecuteWithRetryAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        var maxRetries = _options.MaxRetryCount;
        var baseDelay  = _options.RetryDelay;

        for (var attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                return await operation(cancellationToken);
            }
            catch (SqlException ex) when (attempt < maxRetries && IsTransientSqlError(ex))
            {
                var delay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * (attempt + 1));
                _logger?.LogWarning(
                    "Transient SQL error (attempt {Attempt}/{Max}, code {Code}): {Message}. Retrying in {DelayMs}ms.",
                    attempt + 1, maxRetries, ex.Number, ex.Message, delay.TotalMilliseconds);
                await Task.Delay(delay, cancellationToken);
            }
        }

        // Final attempt — let any exception propagate naturally
        return await operation(cancellationToken);
    }

    private static bool IsTransientSqlError(SqlException ex)
    {
        // 1205 = deadlock victim, -2 = timeout, -1 = connection failure, 20 = general network
        return ex.Number is 1205 or -2 or -1 or 20;
    }

    /// <summary>
    /// Creates the events table if it doesn't exist.
    /// </summary>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        // Ensure the target schema exists before creating any objects within it.
        // This is an idempotent guard — safe to call multiple times.
        var schemaSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_schemaName}')
            BEGIN
                EXEC('CREATE SCHEMA [{_schemaName}] AUTHORIZATION dbo');
            END";

        await using var schemaConn = new SqlConnection(_connectionString);
        await schemaConn.OpenAsync(cancellationToken);
        await using var schemaCmd = new SqlCommand(schemaSql, schemaConn);
        await schemaCmd.ExecuteNonQueryAsync(cancellationToken);

        var sql = $@"
            -- Create SEQUENCE for sequence positions (better performance than MAX query)
            IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'EventSequencePosition' AND schema_id = SCHEMA_ID('{_schemaName}'))
            BEGIN
                CREATE SEQUENCE {_qSequence}
                    START WITH 1
                    INCREMENT BY 1
                    CACHE 1000;  -- Cache 1000 values for better performance
            END

            -- Drop index if it exists (from previous schema versions)
            IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Tags' AND object_id = OBJECT_ID(N'{_qTable}'))
            BEGIN
                DROP INDEX [IX_Tags] ON {_qTable};
            END

            -- Create table if it doesn't exist
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{_qTable}') AND type in (N'U'))
            BEGIN
                CREATE TABLE {_qTable} (
                    [StreamId] NVARCHAR(255) NOT NULL,
                    [Version] BIGINT NOT NULL,
                    [SequencePosition] BIGINT NOT NULL,
                    [EventType] NVARCHAR(500) NOT NULL,
                    [EventData] NVARCHAR(MAX) NOT NULL,
                    [ContentType] NVARCHAR(100) NOT NULL DEFAULT 'application/json',
                    [Tags] NVARCHAR(MAX) NULL,
                    [Metadata] NVARCHAR(MAX) NULL,
                    [Timestamp] DATETIME2 NOT NULL,
                    [PartitionHash] INT NOT NULL DEFAULT 0,
                    PRIMARY KEY ([StreamId], [Version]),
                    INDEX [IX_SequencePosition] ([SequencePosition])
                );
                CREATE UNIQUE NONCLUSTERED INDEX [UX_SequencePosition]
                    ON {_qTable} ([SequencePosition]);
                
                -- Create covering index for ReadStreamAsync (StreamId + Version with included columns)
                CREATE NONCLUSTERED INDEX [IX_Events_StreamId_Version] 
                ON {_qTable} ([StreamId], [Version]) 
                INCLUDE ([SequencePosition], [EventType], [Timestamp]);
                
                -- Create index for timestamp-based queries
                CREATE NONCLUSTERED INDEX [IX_Events_Timestamp] 
                ON {_qTable} ([Timestamp]) 
                INCLUDE ([StreamId], [SequencePosition]);
                
                -- Create covering index for partition-aware queries
                CREATE NONCLUSTERED INDEX [IX_Events_PartitionHash_SequencePosition]
                ON {_qTable}([PartitionHash], [SequencePosition])
                INCLUDE ([StreamId], [Version], [EventType], [EventData], [ContentType], [Tags], [Metadata], [Timestamp]);
            END
            ELSE
            BEGIN
                -- Add ContentType column if it doesn't exist (for existing tables)
                IF NOT EXISTS (SELECT * FROM sys.columns 
                              WHERE object_id = OBJECT_ID(N'{_qTable}') 
                              AND name = 'ContentType')
                BEGIN
                    ALTER TABLE {_qTable}
                    ADD [ContentType] NVARCHAR(100) NULL;
                    
                    -- Backfill existing rows with JSON content type
                    UPDATE {_qTable}
                    SET [ContentType] = 'application/json'
                    WHERE [ContentType] IS NULL;
                    
                    -- Make column NOT NULL with default
                    ALTER TABLE {_qTable}
                    ALTER COLUMN [ContentType] NVARCHAR(100) NOT NULL;
                    
                    ALTER TABLE {_qTable}
                    ADD CONSTRAINT [DF_Events_ContentType] 
                    DEFAULT 'application/json' FOR [ContentType];
                END
                
                -- Add indexes if they don't exist (for existing tables)
                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Events_StreamId_Version' AND object_id = OBJECT_ID(N'{_qTable}'))
                BEGIN
                    CREATE NONCLUSTERED INDEX [IX_Events_StreamId_Version] 
                    ON {_qTable} ([StreamId], [Version]) 
                    INCLUDE ([SequencePosition], [EventType], [Timestamp]);
                END
                
                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Events_Timestamp' AND object_id = OBJECT_ID(N'{_qTable}'))
                BEGIN
                    CREATE NONCLUSTERED INDEX [IX_Events_Timestamp] 
                    ON {_qTable} ([Timestamp]) 
                    INCLUDE ([StreamId], [SequencePosition]);
                END

                -- Ensure SequencePosition is unique before EventTags FK references it.
                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UX_SequencePosition' AND object_id = OBJECT_ID(N'{_qTable}'))
                BEGIN
                    CREATE UNIQUE NONCLUSTERED INDEX [UX_SequencePosition]
                        ON {_qTable}([SequencePosition]);
                END
                
                -- Add PartitionHash column if it doesn't exist (for existing tables).
                -- PartitionHash is computed by C# PartitionHashUtility.GetDeterministicHashCode (FNV-1a)
                -- so it is consistent across all backends. Rows from old schemas default to 0 and
                -- will be excluded from partition-filtered queries until backfilled.
                IF NOT EXISTS (SELECT * FROM sys.columns 
                              WHERE object_id = OBJECT_ID(N'{_qTable}') 
                              AND name = 'PartitionHash')
                BEGIN
                    ALTER TABLE {_qTable}
                    ADD [PartitionHash] INT NOT NULL DEFAULT 0;
                END
                
                -- Add partition filtering index if it doesn't exist (for existing tables)
                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Events_PartitionHash_SequencePosition' AND object_id = OBJECT_ID(N'{_qTable}'))
                BEGIN
                    CREATE NONCLUSTERED INDEX [IX_Events_PartitionHash_SequencePosition]
                    ON {_qTable}([PartitionHash], [SequencePosition])
                    INCLUDE ([StreamId], [Version], [EventType], [EventData], [ContentType], [Tags], [Metadata], [Timestamp]);
                END
            END";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);

        // Create normalized EventTags table for indexed tag lookups
        if (_useEventTagsTable)
        {
            var tagsSql = $@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{_qTags}') AND type in (N'U'))
                BEGIN
                    CREATE TABLE {_qTags} (
                        [GlobalSequencePosition] BIGINT NOT NULL,
                        [Tag]                    NVARCHAR(256) NOT NULL,
                        CONSTRAINT [PK_{_tagsTableName}] PRIMARY KEY CLUSTERED ([Tag], [GlobalSequencePosition]),
                        CONSTRAINT [FK_{_tagsTableName}_Events] FOREIGN KEY ([GlobalSequencePosition])
                            REFERENCES {_qTable}([SequencePosition]) ON DELETE CASCADE
                    );
                    CREATE NONCLUSTERED INDEX [IX_{_tagsTableName}_GlobalSequencePosition]
                        ON {_qTags}([GlobalSequencePosition]);
                END";
            await using var tagsCmd = new SqlCommand(tagsSql, connection);
            await tagsCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create stream registry table if enabled
        if (_enableRegistry)
        {
            var registrySql = $@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{_qRegistry}') AND type in (N'U'))
                BEGIN
                    CREATE TABLE {_qRegistry} (
                        [StreamId] NVARCHAR(255) PRIMARY KEY,
                        [TenantId] NVARCHAR(100) NULL,
                        [AggregateType] NVARCHAR(100) NOT NULL,
                        [AggregateId] UNIQUEIDENTIFIER NULL,
                        [CurrentVersion] BIGINT NOT NULL,
                        [LastSequencePosition] BIGINT NOT NULL,
                        [CreatedAt] DATETIME2 NOT NULL,
                        [LastEventAt] DATETIME2 NOT NULL,
                        [EventCount] BIGINT NOT NULL,
                        [Tags] NVARCHAR(MAX) NULL,
                        [Status] NVARCHAR(20) NOT NULL DEFAULT 'Active'
                    );
                    
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_AggregateType' AND object_id = OBJECT_ID(N'{_qRegistry}'))
                        CREATE INDEX [IX_AggregateType] ON {_qRegistry} ([AggregateType]);
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_TenantId' AND object_id = OBJECT_ID(N'{_qRegistry}'))
                        CREATE INDEX [IX_TenantId] ON {_qRegistry} ([TenantId]);
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_LastSequencePosition' AND object_id = OBJECT_ID(N'{_qRegistry}'))
                        CREATE INDEX [IX_LastSequencePosition] ON {_qRegistry} ([LastSequencePosition]);
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_LastEventAt' AND object_id = OBJECT_ID(N'{_qRegistry}'))
                        CREATE INDEX [IX_LastEventAt] ON {_qRegistry} ([LastEventAt]);
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Status' AND object_id = OBJECT_ID(N'{_qRegistry}'))
                        CREATE INDEX [IX_Status] ON {_qRegistry} ([Status]);
                END
                ELSE
                BEGIN
                    -- Add TenantId column if it doesn't exist (for existing tables)
                    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'{_qRegistry}') AND name = 'TenantId')
                    BEGIN
                        ALTER TABLE {_qRegistry} ADD [TenantId] NVARCHAR(100) NULL;
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_TenantId' AND object_id = OBJECT_ID(N'{_qRegistry}'))
                            CREATE INDEX [IX_TenantId] ON {_qRegistry} ([TenantId]);
                    END
                END";

            try
            {
                await using var registryCommand = new SqlCommand(registrySql, connection);
                await registryCommand.ExecuteNonQueryAsync(cancellationToken);

                _logger?.LogInformation("Initialized stream registry schema for table {TableName}", _registryTableName);
            }
            catch (Microsoft.Data.SqlClient.SqlException ex) when (ex.Number == 2714 || ex.Number == 1913)
            {
                // 2714 = Object already exists, 1913 = Index already exists
                // This can happen in race conditions with multiple nodes initializing simultaneously
                _logger?.LogDebug("Stream registry schema already exists (idempotent check): {Message}", ex.Message);
            }
        }

        var dcbSnapTable = SqlServerSnapshotSchema.ResolveTableName(
            _options.DcbSnapshotsTableName, SqlServerSnapshotSchema.DefaultDcbTable, _tableName);
        var eventSnapTable = SqlServerSnapshotSchema.ResolveTableName(
            _options.EventSnapshotsTableName, SqlServerSnapshotSchema.DefaultEventTable, _tableName);
        await SqlServerSnapshotSchema.EnsureCreatedAsync(
            connection, _schemaName, dcbSnapTable, eventSnapTable, cancellationToken);

        _logger?.LogInformation("Initialized event store schema for table {TableName}", _tableName);
    }

    // Stream Registry Implementation

    public async Task<StreamMetadata?> GetStreamAsync(
        string streamId,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                   CreatedAt, LastEventAt, EventCount, Tags, Status
            FROM {_qRegistry}
            WHERE StreamId = @StreamId";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@StreamId", streamId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return ReadStreamMetadata(reader);
        }

        return null;
    }

    public async Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        if (string.IsNullOrWhiteSpace(aggregateType))
            throw new ArgumentException("Aggregate type cannot be null or empty", nameof(aggregateType));

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                   CreatedAt, LastEventAt, EventCount, Tags, Status
            FROM {_qRegistry}
            WHERE AggregateType = @AggregateType AND Status = 'Active'
            ORDER BY LastEventAt DESC";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@AggregateType", aggregateType);

        var streams = new List<StreamMetadata>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            streams.Add(ReadStreamMetadata(reader));
        }

        return streams.AsReadOnly();
    }

    public async Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        if (string.IsNullOrWhiteSpace(tag))
            throw new ArgumentException("Tag cannot be null or empty", nameof(tag));

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                   CreatedAt, LastEventAt, EventCount, Tags, Status
            FROM {_qRegistry}
            WHERE EXISTS (SELECT 1 FROM OPENJSON(Tags) WHERE value = @Tag)
              AND Status = 'Active'
            ORDER BY LastEventAt DESC";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@Tag", tag);

        var streams = new List<StreamMetadata>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            streams.Add(ReadStreamMetadata(reader));
        }

        return streams.AsReadOnly();
    }

    public async Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT StreamId
            FROM {_qRegistry}
            WHERE Status = 'Active'";

        if (!string.IsNullOrWhiteSpace(prefix))
        {
            sql += " AND StreamId LIKE @Prefix";
        }

        sql += " ORDER BY StreamId";

        await using var command = new SqlCommand(sql, connection);
        if (!string.IsNullOrWhiteSpace(prefix))
        {
            command.Parameters.AddWithValue("@Prefix", prefix + "%");
        }

        var streamIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            streamIds.Add(reader.GetString("StreamId"));
        }

        return streamIds.AsReadOnly();
    }

    public async Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"
            SELECT StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                   CreatedAt, LastEventAt, EventCount, Tags, Status
            FROM {_qRegistry}
            WHERE LastSequencePosition > @AfterSequencePosition
              AND Status = 'Active'
            ORDER BY LastSequencePosition ASC";

        if (limit.HasValue)
        {
            sql += $" OFFSET 0 ROWS FETCH NEXT {limit.Value} ROWS ONLY";
        }

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@AfterSequencePosition", afterSequencePosition);

        var streams = new List<StreamMetadata>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            streams.Add(ReadStreamMetadata(reader));
        }

        return streams.AsReadOnly();
    }

    /// <inheritdoc />
    public async Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"SELECT COUNT_BIG(*) FROM {_qRegistry} WHERE Status = 'Active'";
        if (!string.IsNullOrWhiteSpace(prefix))
            sql += " AND StreamId LIKE @Prefix";

        await using var command = new SqlCommand(sql, connection);
        if (!string.IsNullOrWhiteSpace(prefix))
            command.Parameters.AddWithValue("@Prefix", prefix + "%");

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is long l ? l : Convert.ToInt64(result);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Returns the highest committed <c>SequencePosition</c> in the events table — the true head
    /// of the readable log. These are written-watermark semantics: they avoid treating
    /// SQL Server SEQUENCE cache jumps (allocator head ahead of committed rows) as the
    /// store tail for projection polling.
    /// </remarks>
    public async Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            SELECT ISNULL(MAX(SequencePosition), 0)
            FROM {_qTable}
            """;

        await using var command = new SqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is long l ? l : result is null ? 0L : Convert.ToInt64(result);
    }

    /// <inheritdoc />
    public async Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        if (query == null)
            throw new ArgumentNullException(nameof(query));

        var built = DcbQuerySql.Build(
            _qTable,
            _qTags,
            _useEventTagsTable,
            query,
            fromSequencePosition,
            toSequencePosition,
            toTimestamp,
            limit: null,
            DcbQuerySql.Mode.MaxSequence);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(built.Sql, connection);
        command.Parameters.AddRange(built.Parameters.ToArray());
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is long l ? l : result is null || result is DBNull ? 0L : Convert.ToInt64(result);
    }

    private async Task UpsertStreamMetadataAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        long currentVersion,
        long lastSequencePosition,
        int eventCountDelta,
        IReadOnlyList<string> tags,
        CancellationToken cancellationToken)
    {
        var existing = await GetStreamMetadataAsync(connection, transaction, streamId, cancellationToken);
        var now = DateTime.UtcNow;
        var tenantId = ExtractTenantId(streamId);
        var aggregateType = ExtractAggregateType(streamId);
        var aggregateId = ExtractAggregateId(streamId);
        var tagsJson = JsonSerializer.Serialize(tags, _jsonOptions);

        if (existing == null)
        {
            // Create new stream entry
            var sql = $@"
                INSERT INTO {_qRegistry}
                (StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                 CreatedAt, LastEventAt, EventCount, Tags, Status)
                VALUES
                (@StreamId, @TenantId, @AggregateType, @AggregateId, @CurrentVersion, @LastSequencePosition,
                 @CreatedAt, @LastEventAt, @EventCount, @Tags, @Status)";

            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@StreamId", streamId);
            command.Parameters.AddWithValue("@TenantId", tenantId ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@AggregateType", aggregateType);
            command.Parameters.AddWithValue("@AggregateId", aggregateId.HasValue ? (object)aggregateId.Value : DBNull.Value);
            command.Parameters.AddWithValue("@CurrentVersion", currentVersion);
            command.Parameters.AddWithValue("@LastSequencePosition", lastSequencePosition);
            command.Parameters.AddWithValue("@CreatedAt", now);
            command.Parameters.AddWithValue("@LastEventAt", now);
            command.Parameters.AddWithValue("@EventCount", eventCountDelta);
            command.Parameters.AddWithValue("@Tags", tagsJson);
            command.Parameters.AddWithValue("@Status", "Active");

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        else
        {
            // Update existing stream entry
            var mergedTags = existing.Tags.Union(tags).Distinct().ToList();
            var mergedTagsJson = JsonSerializer.Serialize(mergedTags, _jsonOptions);

            var sql = $@"
                UPDATE {_qRegistry}
                SET CurrentVersion = @CurrentVersion,
                    LastSequencePosition = @LastSequencePosition,
                    LastEventAt = @LastEventAt,
                    EventCount = EventCount + @EventCountDelta,
                    Tags = @Tags
                WHERE StreamId = @StreamId";

            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@StreamId", streamId);
            command.Parameters.AddWithValue("@CurrentVersion", currentVersion);
            command.Parameters.AddWithValue("@LastSequencePosition", lastSequencePosition);
            command.Parameters.AddWithValue("@LastEventAt", now);
            command.Parameters.AddWithValue("@EventCountDelta", eventCountDelta);
            command.Parameters.AddWithValue("@Tags", mergedTagsJson);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task<StreamMetadata?> GetStreamMetadataAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        CancellationToken cancellationToken)
    {
        var sql = $@"
            SELECT StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                   CreatedAt, LastEventAt, EventCount, Tags, Status
            FROM {_qRegistry}
            WHERE StreamId = @StreamId";

        await using var command = new SqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("@StreamId", streamId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return ReadStreamMetadata(reader);
        }

        return null;
    }

    private StreamMetadata ReadStreamMetadata(SqlDataReader reader)
    {
        var tagsJson = reader.IsDBNull("Tags") ? "[]" : reader.GetString("Tags");
        var tags = JsonSerializer.Deserialize<List<string>>(tagsJson, _jsonOptions) ?? new List<string>();

        var streamMetadata = new StreamMetadata
        {
            StreamId = reader.GetString("StreamId"),
            AggregateType = reader.GetString("AggregateType"),
            AggregateId = reader.IsDBNull("AggregateId") ? null : reader.GetGuid("AggregateId"),
            CurrentVersion = reader.GetInt64("CurrentVersion"),
            LastSequencePosition = reader.GetInt64("LastSequencePosition"),
            CreatedAt = reader.GetDateTime("CreatedAt"),
            LastEventAt = reader.GetDateTime("LastEventAt"),
            EventCount = reader.GetInt64("EventCount"),
            Tags = tags,
            Status = Enum.Parse<StreamStatus>(reader.GetString("Status"))
        };
        
        // TenantId column may not exist in older schemas, handle gracefully
        try
        {
            if (!reader.IsDBNull("TenantId"))
            {
                streamMetadata.TenantId = reader.GetString("TenantId");
            }
            else
            {
                // Extract from stream ID if column is null
                streamMetadata.TenantId = ExtractTenantId(streamMetadata.StreamId);
            }
        }
        catch
        {
            // Column doesn't exist, extract from stream ID
            streamMetadata.TenantId = ExtractTenantId(streamMetadata.StreamId);
        }
        
        return streamMetadata;
    }

    private static string ExtractAggregateType(string streamId)
        => StreamIdParser.ExtractAggregateType(streamId);

    private static Guid? ExtractAggregateId(string streamId)
        => StreamIdParser.ExtractAggregateId(streamId);
    
    private static string? ExtractTenantId(string streamId)
        => StreamIdParser.ExtractTenantId(streamId);

    private static Type? ResolveEventType(string typeName)
    {
        if (EventTypeNameResolver.TryResolveType(typeName, out var catalogType))
            return catalogType;

        // Older rows stored CLR FullName or AssemblyQualifiedName.
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var t = assembly.GetType(typeName, throwOnError: false);
                if (t != null) return t;
            }
            catch { /* skip unloadable assemblies */ }
        }

        var commaIndex = typeName.IndexOf(',');
        if (commaIndex > 0)
        {
            var fullName = typeName.Substring(0, commaIndex);

            var directType = Type.GetType(typeName, throwOnError: false);
            if (directType != null) return directType;

            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    var t = assembly.GetType(fullName, throwOnError: false);
                    if (t != null) return t;

                    t = Array.Find(assembly.GetTypes(),
                        x => x.FullName == fullName || x.AssemblyQualifiedName?.StartsWith(fullName + ",", StringComparison.Ordinal) == true);
                    if (t != null) return t;
                }
                catch (ReflectionTypeLoadException ex)
                {
                    if (ex.Types == null) continue;
                    foreach (var loaded in ex.Types)
                    {
                        if (loaded?.FullName == fullName) return loaded;
                    }
                }
                catch { /* skip unloadable assemblies */ }
            }
        }

        // Token on disk, type not yet Warmup'd: match [EventTypeName] without GetName.
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                foreach (var candidate in assembly.GetTypes())
                {
                    if (!typeof(IEvent).IsAssignableFrom(candidate) || candidate.IsAbstract || candidate.IsInterface)
                        continue;

                    if (string.Equals(EventTypeNameResolver.TryGetDeclaredName(candidate), typeName, StringComparison.Ordinal))
                        return candidate;
                }
            }
            catch (ReflectionTypeLoadException ex)
            {
                if (ex.Types == null) continue;
                foreach (var candidate in ex.Types)
                {
                    if (candidate == null || !typeof(IEvent).IsAssignableFrom(candidate) || candidate.IsAbstract || candidate.IsInterface)
                        continue;

                    if (string.Equals(EventTypeNameResolver.TryGetDeclaredName(candidate), typeName, StringComparison.Ordinal))
                        return candidate;
                }
            }
            catch { /* skip unloadable assemblies */ }
        }

        return null;
    }

    /// <summary>
    /// Writes events to the outbox table in the same transaction.
    /// This ensures atomicity between event store and outbox writes.
    /// </summary>
    private async Task WriteEventsToOutboxAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        List<IEvent> events,
        List<long> sequencePositions,
        string streamId,
        EventMetadata? metadata,
        List<string> tags,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < events.Count; i++)
        {
            var @event = events[i];
            var sequencePosition = sequencePositions[i];

            var outboxMessage = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = EventTypeNameResolver.GetName(@event.GetType()),
                Payload = JsonSerializer.Serialize(@event, @event.GetType(), _jsonOptions),
                Metadata = metadata != null ? JsonSerializer.Serialize(metadata, _jsonOptions) : "{}",
                StreamId = streamId,
                SequencePosition = sequencePosition,
                CreatedAt = DateTime.UtcNow,
                Attempts = 0
            };

            // Use SQL directly to write outbox message with same connection/transaction
            var sql = $@"
                INSERT INTO [{_schemaName}].[{_options.OutboxTableName}] (
                    Id, EventType, Payload, Metadata, CreatedAt, 
                    Attempts, StreamId, SequencePosition
                )
                VALUES (
                    @Id, @EventType, @Payload, @Metadata, @CreatedAt,
                    @Attempts, @StreamId, @SequencePosition
                )";

            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@Id", outboxMessage.Id);
            command.Parameters.AddWithValue("@EventType", outboxMessage.EventType);
            command.Parameters.AddWithValue("@Payload", outboxMessage.Payload);
            command.Parameters.AddWithValue("@Metadata", outboxMessage.Metadata);
            command.Parameters.AddWithValue("@CreatedAt", outboxMessage.CreatedAt);
            command.Parameters.AddWithValue("@Attempts", outboxMessage.Attempts);
            command.Parameters.AddWithValue("@StreamId", outboxMessage.StreamId);
            command.Parameters.AddWithValue("@SequencePosition", outboxMessage.SequencePosition);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        _logger?.LogDebug(
            "Wrote {Count} events to outbox for stream {StreamId}",
            events.Count,
            streamId);
    }

    // ── IEventStoreSubscriptions (portable; poll-backed live) ─────────────────

    /// <inheritdoc />
    public ISubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(subscriberId))
            throw new ArgumentException("Subscriber id is required.", nameof(subscriberId));
        if (fromSequence < 0)
            throw new ArgumentOutOfRangeException(nameof(fromSequence), "fromSequence cannot be negative.");

        if (_options.SubscriptionChannelCapacity < 1)
            throw new InvalidOperationException("SubscriptionChannelCapacity must be at least 1.");
        if (_options.SqlSubscriptionBatchSize < 1)
            throw new InvalidOperationException("SqlSubscriptionBatchSize must be at least 1.");
        if (_options.SqlSubscriptionPollInterval < TimeSpan.Zero)
            throw new InvalidOperationException("SqlSubscriptionPollInterval cannot be negative.");

        filter ??= EventSubscriptionFilter.All();

        var handle = new SqlServerSubscriptionHandle(
            subscriberId,
            filter,
            _options.SubscriptionChannelCapacity,
            fromSequence,
            cancellationToken,
            h => _subscriptions.TryRemove(h, out _));

        if (!_subscriptions.TryAdd(handle, 0))
            throw new InvalidOperationException("Failed to register subscription handle.");

        _ = Task.Run(() => RunSubscriptionAsync(handle), CancellationToken.None);
        return handle;
    }

    private async Task RunSubscriptionAsync(SqlServerSubscriptionHandle handle)
    {
        var ct = handle.CancellationToken;
        long lastDelivered = handle.FromSequence - 1;
        var batchSize = _options.SqlSubscriptionBatchSize;
        var pollInterval = _options.SqlSubscriptionPollInterval;
        var readQuery = ResolveSubscriptionQuery(handle.Filter);

        try
        {
            while (!ct.IsCancellationRequested && !handle.IsDisposed)
            {
                var delivered = 0;
                var from = lastDelivered + 1;

                await foreach (var evt in ReadByQueryStreamAsync(readQuery, fromSequencePosition: from, cancellationToken: ct)
                    .ConfigureAwait(false))
                {
                    if (ct.IsCancellationRequested || handle.IsDisposed)
                        break;

                    if (evt.SequencePosition <= lastDelivered)
                        continue;

                    if (!handle.Filter.Matches(evt))
                        continue;

                    await handle.WriteAsync(evt, ct).ConfigureAwait(false);
                    lastDelivered = evt.SequencePosition;
                    delivered++;

                    if (delivered >= batchSize)
                        break;
                }

                if (delivered > 0)
                    continue; // More pages may remain — keep catching up without idle delay.

                // Idle live wait (poll-backed).
                try
                {
                    await Task.Delay(pollInterval, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Normal cancel / dispose path.
        }
        catch (ChannelClosedException)
        {
            // Handle disposed while writing.
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "SQL Server subscription {SubscriberId} failed.", handle.SubscriberId);
        }
        finally
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static Query ResolveSubscriptionQuery(EventSubscriptionFilter filter) =>
        filter.Kind switch
        {
            EventSubscriptionScope.Query => filter.Query ?? Query.All(),
            // Stream / All: scan by global sequence; Stream filter applied via Filter.Matches.
            _ => Query.All()
        };
}

