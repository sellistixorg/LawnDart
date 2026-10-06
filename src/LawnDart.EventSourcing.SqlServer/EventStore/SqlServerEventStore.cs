using System.Collections.Concurrent;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text;
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
using LawnDart.Sql;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// SQL Server <see cref="IEventLog"/>. Typed <see cref="IEventStore"/> methods
/// forward to <see cref="EventStoreAdapter"/>.
/// </summary>
public class SqlServerEventStore : IEventStore, IEventStoreSubscriptions, IEventLog, IEventLogSubscriptions
{
    internal const string StreamEventColumns =
        "StreamId, Version, SequencePosition, EventTypeId, EventData, SchemaVersion, CodecId, Tags, Metadata, Timestamp";

    /// <summary>Extended property written on a fresh events table. Mismatch is a wipe.</summary>
    internal const string SchemaFormatPropertyName = "LawnDart_SchemaFormat";

    /// <summary>CLN-06 shape: EventTypeId INT FK, EventTypes lookup, no EventType column.</summary>
    internal const string CurrentSchemaFormat = "2";

    internal const string PartitionHashIndexName = "IX_Events_PartitionHash_SequencePosition";

    internal string EventTypesTableName => _typesTableName;

    private readonly EventSession _session;
    private readonly EventStoreAdapter _adapter;
    private readonly string _connectionString;
    private readonly string _tableName;
    private readonly string _registryTableName;
    private readonly string _tagsTableName;
    private readonly string _typesTableName;
    private readonly bool _enableRegistry;
    private readonly EventTypeIdCache _eventTypes = new();
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
    private readonly string _qTypes;
    private readonly string _qSequence;

    internal string QualifiedEventsTable => _qTable;

    internal string QualifiedRegistryTable => _qRegistry;

    internal string QualifiedTagsTable => _qTags;

    internal string QualifiedTypesTable => _qTypes;

    internal string QualifiedSequence => _qSequence;

    public SqlServerEventStore(
        string connectionString,
        IEventSerializer? serializer = null,
        string tableName = "Events",
        string registryTableName = "Streams",
        bool enableRegistry = true,
        SqlServerEventStoreOptions? options = null,
        IOutboxWriter? outboxWriter = null,
        ILogger<SqlServerEventStore>? logger = null,
        EventSession? session = null)
    {
        _session = session ?? new EventSession(
            serializer ?? new JsonEventSerializer(),
            new WriteThroughEventTypeCatalog());
        _options = options ?? new SqlServerEventStoreOptions();
        _adapter = new EventStoreAdapter(this, _session, this, _options.SubscriptionChannelCapacity);
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _registryTableName = registryTableName ?? throw new ArgumentNullException(nameof(registryTableName));
        _enableRegistry = enableRegistry;
        // If caller uses a custom events table but leaves tags table at default, derive a per-table
        // tags table name to avoid cross-table FK coupling in tests or multi-tenant schemas.
        _tagsTableName =
            string.Equals(_options.EventTagsTableName, "EventTags", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(_tableName, "Events", StringComparison.OrdinalIgnoreCase)
                ? $"EventTags_{_tableName}"
                : _options.EventTagsTableName;
        _typesTableName =
            !string.Equals(_tableName, "Events", StringComparison.OrdinalIgnoreCase)
                ? $"EventTypes_{_tableName}"
                : "EventTypes";
        _ = outboxWriter;
        _logger = logger;

        // Pre-compute schema-qualified names (SchemaName defaults to "dbo" for backward compat)
        _schemaName  = string.IsNullOrWhiteSpace(_options.SchemaName) ? "dbo" : _options.SchemaName;
        ContextName  = string.IsNullOrWhiteSpace(_options.ContextName) ? "default" : _options.ContextName;
        _qTable      = SqlIdentifier.Qualify(_schemaName, _tableName);
        _qRegistry  = SqlIdentifier.Qualify(_schemaName, _registryTableName);
        _qTags      = SqlIdentifier.Qualify(_schemaName, _tagsTableName);
        _qTypes     = SqlIdentifier.Qualify(_schemaName, _typesTableName);
        _qSequence  = SqlIdentifier.Qualify(_schemaName, "EventSequencePosition");

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

    public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _adapter.ReadStreamAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _adapter.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _adapter.ReadByQueryAsync(query, fromSequencePosition, limit, toSequencePosition, toTimestamp, cancellationToken);

    public IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _adapter.ReadByQueryStreamAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    public Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
        => _adapter.AppendAsync(streamId, events, expectedVersion, metadata, tags, cancellationToken);

    public Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
        => _adapter.AppendAsync(events, condition, metadata, tags, cancellationToken);

    private async Task<long> GetCurrentVersionAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        CancellationToken cancellationToken)
    {
        // Optimize: Use registry if available (much faster than MAX query)
        if (_enableRegistry)
        {
            var metadata = await GetStreamMetadataAsync(connection, transaction, streamId, cancellationToken).ConfigureAwait(false);
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

        var command = new SqlCommand(sql, connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@StreamId", streamId);

        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result != null && result != DBNull.Value ? Convert.ToInt64(result) : -1;
    }

    private async Task<long> GetNextSequencePositionAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        // Use SEQUENCE object for better performance (no table lock)
        var sql = $"SELECT NEXT VALUE FOR {_qSequence}";

        var command = new SqlCommand(sql, connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);

        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result);
    }

    private async Task<IReadOnlyList<long>> InsertEventsBatchAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        long startingVersion,
        IReadOnlyList<AppendEvent> envelopes,
        CancellationToken cancellationToken)
    {
        // 11 parameters per frame; stay under SQL Server's 2100-parameter limit.
        const int maxEventsPerBatch = 190;

        var allSequencePositions = new List<long>();
        var commitTimestamp = DateTime.UtcNow;
        var partitionHash = PartitionHashUtility.GetDeterministicHashCode(streamId);

        for (int chunkStart = 0; chunkStart < envelopes.Count; chunkStart += maxEventsPerBatch)
        {
            var chunkEnd = Math.Min(chunkStart + maxEventsPerBatch, envelopes.Count);
            var chunkSize = chunkEnd - chunkStart;

            var sequencePositions = new List<long>(chunkSize);
            for (int i = 0; i < chunkSize; i++)
            {
                sequencePositions.Add(await GetNextSequencePositionAsync(connection, transaction, cancellationToken).ConfigureAwait(false));
            }

            var values = new List<string>(chunkSize);
            var parameters = new List<SqlParameter>();

            for (int i = 0; i < chunkSize; i++)
            {
                var eventIndex = chunkStart + i;
                var envelope = envelopes[eventIndex];
                var version = startingVersion + eventIndex;
                var metadataText = envelope.Metadata.IsEmpty
                    ? null
                    : Encoding.UTF8.GetString(envelope.Metadata.Span);
                var tagsJson = JsonSerializer.Serialize(envelope.Tags, _jsonOptions);

                var streamIdParam = $"@StreamId{eventIndex}";
                var versionParam = $"@Version{eventIndex}";
                var seqPosParam = $"@SequencePosition{eventIndex}";
                var eventTypeIdParam = $"@EventTypeId{eventIndex}";
                var eventDataParam = $"@EventData{eventIndex}";
                var schemaVersionParam = $"@SchemaVersion{eventIndex}";
                var codecIdParam = $"@CodecId{eventIndex}";
                var tagsParam = $"@Tags{eventIndex}";
                var metadataParam = $"@Metadata{eventIndex}";
                var timestampParam = $"@Timestamp{eventIndex}";
                var partitionHashParam = $"@PartitionHash{eventIndex}";

                values.Add($"({streamIdParam}, {versionParam}, {seqPosParam}, {eventTypeIdParam}, {eventDataParam}, {schemaVersionParam}, {codecIdParam}, {tagsParam}, {metadataParam}, {timestampParam}, {partitionHashParam})");

                if (!_eventTypes.TryGetId(envelope.EventType, out var eventTypeId))
                {
                    throw new InvalidOperationException(
                        $"Event type token '{envelope.EventType}' is not in {_qTypes}. " +
                        "Foreign tokens must be registered before the append transaction opens.");
                }

                parameters.Add(new SqlParameter(streamIdParam, streamId));
                parameters.Add(new SqlParameter(versionParam, version));
                parameters.Add(new SqlParameter(seqPosParam, sequencePositions[i]));
                parameters.Add(new SqlParameter(eventTypeIdParam, eventTypeId));
                parameters.Add(PayloadParameter(eventDataParam, envelope.Payload));
                parameters.Add(new SqlParameter(schemaVersionParam, envelope.SchemaVersion));
                parameters.Add(new SqlParameter(codecIdParam, SqlDbType.TinyInt) { Value = envelope.CodecId });
                parameters.Add(new SqlParameter(tagsParam, tagsJson));
                parameters.Add(new SqlParameter(metadataParam, (object?)metadataText ?? DBNull.Value));
                parameters.Add(new SqlParameter(timestampParam, commitTimestamp));
                parameters.Add(new SqlParameter(partitionHashParam, partitionHash));
            }

            var sql = $@"
                INSERT INTO {_qTable}
                (StreamId, Version, SequencePosition, EventTypeId, EventData, SchemaVersion, CodecId, Tags, Metadata, Timestamp, PartitionHash)
                VALUES {string.Join(", ", values)}";

            var command = new SqlCommand(sql, connection, transaction);
            await using var commandDisposal = command.ConfigureAwait(false);
            command.Parameters.AddRange(parameters.ToArray());
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await InsertEventTagsAsync(connection, transaction, envelopes, chunkStart, chunkSize, sequencePositions, cancellationToken)
                .ConfigureAwait(false);

            allSequencePositions.AddRange(sequencePositions);
        }

        return allSequencePositions.AsReadOnly();
    }

    private async Task InsertEventTagsAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyList<AppendEvent> envelopes,
        int chunkStart,
        int chunkSize,
        IReadOnlyList<long> sequencePositions,
        CancellationToken cancellationToken)
    {
        var tagValues = new List<string>();
        var tagParameters = new List<SqlParameter>();
        var tagIdx = 0;
        for (var i = 0; i < chunkSize; i++)
        {
            foreach (var tag in envelopes[chunkStart + i].Tags)
            {
                tagValues.Add($"(@SeqPos{tagIdx}, @TagVal{tagIdx})");
                tagParameters.Add(new SqlParameter($"@SeqPos{tagIdx}", sequencePositions[i]));
                tagParameters.Add(new SqlParameter($"@TagVal{tagIdx}", tag));
                tagIdx++;
            }
        }

        if (tagValues.Count == 0)
            return;

        var tagSql = $@"
            INSERT INTO {_qTags} (GlobalSequencePosition, Tag)
            VALUES {string.Join(", ", tagValues)}";
        var tagCmd = new SqlCommand(tagSql, connection, transaction);
        await using var tagCmdDisposal = tagCmd.ConfigureAwait(false);
        tagCmd.Parameters.AddRange(tagParameters.ToArray());
        await tagCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static SqlParameter PayloadParameter(string name, ReadOnlyMemory<byte> payload)
    {
        var bytes = payload.IsEmpty ? Array.Empty<byte>() : payload.ToArray();
        return new SqlParameter(name, SqlDbType.VarBinary, -1) { Value = bytes };
    }

    private readonly record struct EventRow(
        string StreamId,
        long Version,
        long SequencePosition,
        int EventTypeId,
        byte[] Payload,
        int SchemaVersion,
        byte CodecId,
        string TagsJson,
        string MetadataJson,
        DateTime Timestamp);

    private static EventRow ReadEventRow(SqlDataReader reader)
        => new(
            reader.GetString("StreamId"),
            reader.GetInt64("Version"),
            reader.GetInt64("SequencePosition"),
            reader.GetInt32("EventTypeId"),
            (byte[])reader.GetValue(reader.GetOrdinal("EventData")),
            reader.GetInt32("SchemaVersion"),
            reader.GetByte(reader.GetOrdinal("CodecId")),
            reader.IsDBNull(reader.GetOrdinal("Tags")) ? "[]" : reader.GetString("Tags"),
            reader.IsDBNull(reader.GetOrdinal("Metadata")) ? string.Empty : reader.GetString("Metadata"),
            reader.GetDateTime("Timestamp"));

    private async Task<IReadOnlyList<RecordedEvent>> MaterializeAsync(
        IReadOnlyList<EventRow> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
            return [];

        await EnsureTokensCachedAsync(rows.Select(r => r.EventTypeId), cancellationToken).ConfigureAwait(false);

        var frames = new List<RecordedEvent>(rows.Count);
        foreach (var row in rows)
            frames.Add(ToRecordedEvent(row));
        return frames;
    }

    private RecordedEvent ToRecordedEvent(EventRow row)
    {
        if (!_eventTypes.TryGetToken(row.EventTypeId, out var eventType))
        {
            throw new InvalidOperationException(
                $"EventTypeId {row.EventTypeId} is not in {_qTypes} after a cache refresh. " +
                "Drop and recreate the event store schema.");
        }

        var tags = JsonSerializer.Deserialize<string[]>(row.TagsJson, _jsonOptions) ?? [];
        var metadataBytes = string.IsNullOrEmpty(row.MetadataJson)
            ? ReadOnlyMemory<byte>.Empty
            : Encoding.UTF8.GetBytes(row.MetadataJson);

        return new RecordedEvent(
            eventType,
            row.Payload,
            row.StreamId,
            row.Version,
            row.SequencePosition,
            row.Timestamp,
            metadataBytes,
            row.SchemaVersion,
            row.CodecId,
            tags);
    }

    private static IReadOnlyList<string> UnionTags(IReadOnlyList<AppendEvent> envelopes)
        => envelopes.SelectMany(e => e.Tags).Distinct(StringComparer.Ordinal).ToList();

    /// <summary>
    /// Takes <c>UPDLOCK, HOLDLOCK</c> on each condition tag (sorted) in
    /// <c>EventTags</c> so a concurrent insert of the same tag cannot sneak in
    /// between the existence check and commit. Tagless conditions lock
    /// <c>Events</c> the same way.
    /// </summary>
    private async Task AcquireDcbFenceLocksAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        AppendCondition condition,
        CancellationToken cancellationToken)
    {
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

            var eventsCmd = new SqlCommand(eventsSql, connection, transaction);
            await using var eventsCmdDisposal = eventsCmd.ConfigureAwait(false);
            eventsCmd.Parameters.AddRange(eventsParams.ToArray());
            await eventsCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        foreach (var tag in tags)
        {
            var sql = $@"
                SELECT TOP 1 1
                FROM {_qTags} WITH (UPDLOCK, HOLDLOCK, ROWLOCK)
                WHERE Tag = @Tag";
            var command = new SqlCommand(sql, connection, transaction);
            await using var commandDisposal = command.ConfigureAwait(false);
            command.Parameters.Add(new SqlParameter("@Tag", tag));
            if (condition.After.HasValue)
            {
                command.CommandText += " AND GlobalSequencePosition > @After";
                command.Parameters.Add(new SqlParameter("@After", condition.After.Value));
            }

            await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
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
        var sql = $@"
            SELECT TOP 1 1
            FROM {_qTable} e
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
                    var typeIds = new List<int>(item.Types.Count);
                    foreach (var typeName in item.Types)
                    {
                        if (_eventTypes.TryGetId(typeName, out var typeId))
                            typeIds.Add(typeId);
                    }

                    if (typeIds.Count == 0)
                    {
                        conditionParts.Add("1 = 0");
                    }
                    else
                    {
                        var names = new string[typeIds.Count];
                        for (int j = 0; j < typeIds.Count; j++)
                        {
                            var paramName = $"@Type{i}_{j}";
                            names[j] = paramName;
                            parameters.Add(new SqlParameter(paramName, typeIds[j]));
                        }

                        conditionParts.Add($"e.EventTypeId IN ({string.Join(", ", names)})");
                    }
                }

                if (item.Tags != null && item.Tags.Count > 0)
                {
                    for (int j = 0; j < item.Tags.Count; j++)
                    {
                        var tag = item.Tags[j];
                        var paramName = $"@Tag{i}_{j}";
                        conditionParts.Add($"EXISTS (SELECT 1 FROM {_qTags} et WHERE et.GlobalSequencePosition = e.SequencePosition AND et.Tag = {paramName})");
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

        var command = new SqlCommand(sql, connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddRange(parameters.ToArray());
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
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
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (SqlException ex) when (attempt < maxRetries && IsTransientSqlError(ex))
            {
                var delay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * (attempt + 1));
                if (_logger is not null)
                    SqlServerEventStoreLog.TransientSqlError(
                        _logger, attempt + 1, maxRetries, ex.Number, ex.Message, delay.TotalMilliseconds);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        // Final attempt — let any exception propagate naturally
        return await operation(cancellationToken).ConfigureAwait(false);
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
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{SqlIdentifier.Literal(_schemaName)}')
            BEGIN
                EXEC('CREATE SCHEMA {SqlIdentifier.Literal(SqlIdentifier.Quote(_schemaName))} AUTHORIZATION dbo');
            END";

        var schemaConn = new SqlConnection(_connectionString);
        await using var schemaConnDisposal = schemaConn.ConfigureAwait(false);
        await schemaConn.OpenAsync(cancellationToken).ConfigureAwait(false);
        var schemaCmd = new SqlCommand(schemaSql, schemaConn);
        await using var schemaCmdDisposal = schemaCmd.ConfigureAwait(false);
        await schemaCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        await EnsureSequenceAsync(connection, cancellationToken).ConfigureAwait(false);
        await ThrowIfIncompatibleEventsTableAsync(connection, cancellationToken).ConfigureAwait(false);
        await EnsureEventTypesTableAsync(connection, cancellationToken).ConfigureAwait(false);
        await EnsureEventsTableAsync(connection, cancellationToken).ConfigureAwait(false);
        await WarmEventTypesAsync(connection, cancellationToken).ConfigureAwait(false);

        var tagsSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qTags)}') AND type in (N'U'))
            BEGIN
                CREATE TABLE {_qTags} (
                    [GlobalSequencePosition] BIGINT NOT NULL,
                    [Tag]                    NVARCHAR(256) NOT NULL,
                    CONSTRAINT {SqlIdentifier.Quote("PK_" + _tagsTableName)} PRIMARY KEY CLUSTERED ([Tag], [GlobalSequencePosition]),
                    CONSTRAINT {SqlIdentifier.Quote("FK_" + _tagsTableName + "_Events")} FOREIGN KEY ([GlobalSequencePosition])
                        REFERENCES {_qTable}([SequencePosition]) ON DELETE CASCADE
                );
                CREATE NONCLUSTERED INDEX {SqlIdentifier.Quote("IX_" + _tagsTableName + "_GlobalSequencePosition")}
                    ON {_qTags}([GlobalSequencePosition]);
            END";
        var tagsCmd = new SqlCommand(tagsSql, connection);
        await using var tagsCmdDisposal = tagsCmd.ConfigureAwait(false);
        await tagsCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        // Create stream registry table if enabled
        if (_enableRegistry)
        {
            var registrySql = $@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}') AND type in (N'U'))
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
                    
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_AggregateType' AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}'))
                        CREATE INDEX [IX_AggregateType] ON {_qRegistry} ([AggregateType]);
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_TenantId' AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}'))
                        CREATE INDEX [IX_TenantId] ON {_qRegistry} ([TenantId]);
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_LastSequencePosition' AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}'))
                        CREATE INDEX [IX_LastSequencePosition] ON {_qRegistry} ([LastSequencePosition]);
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_LastEventAt' AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}'))
                        CREATE INDEX [IX_LastEventAt] ON {_qRegistry} ([LastEventAt]);
                    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Status' AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}'))
                        CREATE INDEX [IX_Status] ON {_qRegistry} ([Status]);
                END
                ELSE
                BEGIN
                    -- Add TenantId column if it doesn't exist (for existing tables)
                    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}') AND name = 'TenantId')
                    BEGIN
                        ALTER TABLE {_qRegistry} ADD [TenantId] NVARCHAR(100) NULL;
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_TenantId' AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}'))
                            CREATE INDEX [IX_TenantId] ON {_qRegistry} ([TenantId]);
                    END
                END";

            try
            {
                var registryCommand = new SqlCommand(registrySql, connection);
                await using var registryCommandDisposal = registryCommand.ConfigureAwait(false);
                await registryCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

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
            connection, _schemaName, dcbSnapTable, eventSnapTable, cancellationToken).ConfigureAwait(false);

        _logger?.LogInformation("Initialized event store schema for table {TableName}", _tableName);
    }

    private async Task EnsureSequenceAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        var sql = $@"
            IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'EventSequencePosition' AND schema_id = SCHEMA_ID('{SqlIdentifier.Literal(_schemaName)}'))
            BEGIN
                CREATE SEQUENCE {_qSequence}
                    START WITH 1
                    INCREMENT BY 1
                    CACHE 1000;
            END";
        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ThrowIfIncompatibleEventsTableAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        var existsCmd = new SqlCommand(
            $"SELECT CASE WHEN OBJECT_ID(N'{SqlIdentifier.Literal(_qTable)}', 'U') IS NULL THEN 0 ELSE 1 END",
            connection);
        await using var existsCmdDisposal = existsCmd.ConfigureAwait(false);
        var exists = Convert.ToInt32(await existsCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) == 1;
        if (!exists)
            return;

        var found = await ReadSchemaFormatAsync(connection, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(found, CurrentSchemaFormat, StringComparison.Ordinal))
            throw new IncompatibleEventStoreSchemaException(_qTable, found, CurrentSchemaFormat);
    }

    private async Task EnsureEventsTableAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        var existsCmd = new SqlCommand(
            $"SELECT CASE WHEN OBJECT_ID(N'{SqlIdentifier.Literal(_qTable)}', 'U') IS NULL THEN 0 ELSE 1 END",
            connection);
        await using (existsCmd.ConfigureAwait(false))
        {
            var exists = Convert.ToInt32(await existsCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) == 1;
            if (exists)
                return;
        }

        var readable = SqlIdentifier.Qualify(_schemaName, _tableName + "_Readable");
        var sql = $@"
            CREATE TABLE {_qTable} (
                [StreamId] NVARCHAR(255) NOT NULL,
                [Version] BIGINT NOT NULL,
                [SequencePosition] BIGINT NOT NULL,
                [EventTypeId] INT NOT NULL,
                [EventData] VARBINARY(MAX) NOT NULL,
                [SchemaVersion] INT NOT NULL,
                [CodecId] TINYINT NOT NULL,
                [Tags] NVARCHAR(4000) NULL,
                [Metadata] NVARCHAR(MAX) NULL,
                [Timestamp] DATETIME2 NOT NULL,
                [PartitionHash] INT NOT NULL,
                PRIMARY KEY ([StreamId], [Version]),
                CONSTRAINT {SqlIdentifier.Quote("FK_" + _tableName + "_EventTypes")} FOREIGN KEY ([EventTypeId])
                    REFERENCES {_qTypes}([Id])
            );
            CREATE UNIQUE NONCLUSTERED INDEX [UX_SequencePosition]
                ON {_qTable} ([SequencePosition]);
            CREATE NONCLUSTERED INDEX [IX_Events_StreamId_Version]
                ON {_qTable} ([StreamId], [Version])
                INCLUDE ([SequencePosition], [EventTypeId], [Timestamp]);
            CREATE NONCLUSTERED INDEX [IX_Events_Timestamp]
                ON {_qTable} ([Timestamp])
                INCLUDE ([StreamId], [SequencePosition]);
            CREATE NONCLUSTERED INDEX [{PartitionHashIndexName}]
                ON {_qTable}([PartitionHash], [SequencePosition])
                INCLUDE ([StreamId], [Version], [EventTypeId], [CodecId], [SchemaVersion], [Timestamp]);
            EXEC('CREATE VIEW {SqlIdentifier.Literal(readable)} AS
                SELECT
                    e.StreamId,
                    e.Version,
                    e.SequencePosition,
                    e.EventTypeId,
                    t.Token AS EventType,
                    CAST(e.EventData AS VARCHAR(MAX)) AS EventJson,
                    e.SchemaVersion,
                    e.CodecId,
                    e.Tags,
                    e.Metadata,
                    e.[Timestamp],
                    e.PartitionHash
                FROM {SqlIdentifier.Literal(_qTable)} e
                INNER JOIN {SqlIdentifier.Literal(_qTypes)} t ON t.Id = e.EventTypeId');
            EXEC sys.sp_addextendedproperty
                @name = N'{SchemaFormatPropertyName}',
                @value = N'{CurrentSchemaFormat}',
                @level0type = N'SCHEMA', @level0name = N'{SqlIdentifier.Literal(_schemaName)}',
                @level1type = N'TABLE',  @level1name = N'{SqlIdentifier.Literal(_tableName)}';";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureEventTypesTableAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        var sql = $@"
            IF OBJECT_ID(N'{SqlIdentifier.Literal(_qTypes)}', 'U') IS NULL
            BEGIN
                CREATE TABLE {_qTypes} (
                    [Id] INT IDENTITY(1, 1) NOT NULL,
                    [Token] NVARCHAR(500) NOT NULL,
                    CONSTRAINT {SqlIdentifier.Quote("PK_" + _typesTableName)} PRIMARY KEY ([Id]),
                    CONSTRAINT {SqlIdentifier.Quote("UX_" + _typesTableName + "_Token")} UNIQUE ([Token])
                );
            END";
        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task WarmEventTypesAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        var tokens = CatalogFamilyTokens().Distinct(StringComparer.Ordinal).ToList();
        if (tokens.Count > 0)
        {
            var transaction = ((SqlTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
            await using var transactionDisposal = transaction.ConfigureAwait(false);
            try
            {
                foreach (var token in tokens.OrderBy(t => t, StringComparer.Ordinal))
                    await GetOrInsertEventTypeIdCoreAsync(connection, transaction, token, cancellationToken)
                        .ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                try { await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* already rolled back */ }
                throw;
            }
        }

        await ReloadEventTypeCacheAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    private IEnumerable<string> CatalogFamilyTokens()
    {
        if (_session.Catalog is EventTypeCatalog catalog)
        {
            foreach (var family in catalog.EnumerateFamilies())
                yield return family.Token;
        }
    }

    private async Task ReloadEventTypeCacheAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        var rows = new List<(int Id, string Token)>();
        var command = new SqlCommand($"SELECT Id, Token FROM {_qTypes}", connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            rows.Add((reader.GetInt32(0), reader.GetString(1)));
        _eventTypes.ReplaceAll(rows);
    }

    private async Task EnsureTokensCachedAsync(IEnumerable<int> typeIds, CancellationToken cancellationToken)
    {
        var missing = false;
        foreach (var id in typeIds)
        {
            if (!_eventTypes.TryGetToken(id, out _))
            {
                missing = true;
                break;
            }
        }

        if (!missing)
            return;

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ReloadEventTypeCacheAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureEventTypeIdsAsync(
        IReadOnlyList<AppendEvent> envelopes,
        CancellationToken cancellationToken)
    {
        var tokens = envelopes
            .Select(e => e.EventType)
            .Where(t => !_eventTypes.TryGetId(t, out _))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();

        foreach (var token in tokens)
            await RegisterEventTypeTokenAsync(token, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Get-or-insert <paramref name="token"/> in its own short transaction, committed
    /// before the caller opens the append transaction.
    /// </summary>
    private async Task RegisterEventTypeTokenAsync(string token, CancellationToken cancellationToken)
    {
        if (_eventTypes.TryGetId(token, out _))
            return;

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var transaction = ((SqlTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false));
        await using var transactionDisposal = transaction.ConfigureAwait(false);
        try
        {
            var id = await GetOrInsertEventTypeIdCoreAsync(connection, transaction, token, cancellationToken)
                .ConfigureAwait(false);
            _eventTypes.Add(id, token);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException ex) when (ex.Number is 2627 or 2601)
        {
            try { await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* already rolled back */ }
            var select = new SqlCommand(
                $"SELECT Id FROM {_qTypes} WHERE Token = @Token",
                connection);
            await using var selectDisposal = select.ConfigureAwait(false);
            select.Parameters.Add(new SqlParameter("@Token", token));
            var id = Convert.ToInt32(await select.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
            _eventTypes.Add(id, token);
        }
        catch
        {
            try { await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* already rolled back */ }
            throw;
        }
    }

    private async Task<int> GetOrInsertEventTypeIdCoreAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string token,
        CancellationToken cancellationToken)
    {
        var select = new SqlCommand(
            $"""
            SELECT Id
            FROM {_qTypes} WITH (UPDLOCK, HOLDLOCK, ROWLOCK)
            WHERE Token = @Token
            """,
            connection,
            transaction);
        await using (select.ConfigureAwait(false))
        {
            select.Parameters.Add(new SqlParameter("@Token", token));
            var existing = await select.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (existing is not null and not DBNull)
                return Convert.ToInt32(existing);
        }

        var insert = new SqlCommand(
            $"""
            INSERT INTO {_qTypes} (Token)
            OUTPUT INSERTED.Id
            VALUES (@Token)
            """,
            connection,
            transaction);
        await using var insertDisposal = insert.ConfigureAwait(false);
        insert.Parameters.Add(new SqlParameter("@Token", token));
        return Convert.ToInt32(await insert.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
    }

    private async Task<string?> ReadSchemaFormatAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT CAST(value AS nvarchar(128))
            FROM sys.extended_properties
            WHERE major_id = OBJECT_ID(@Table)
              AND name = @Name
              AND minor_id = 0
            """;
        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@Table", _qTable);
        command.Parameters.AddWithValue("@Name", SchemaFormatPropertyName);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull ? null : Convert.ToString(result);
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

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
            SELECT StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                   CreatedAt, LastEventAt, EventCount, Tags, Status
            FROM {_qRegistry}
            WHERE StreamId = @StreamId";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@StreamId", streamId);

        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
            SELECT StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                   CreatedAt, LastEventAt, EventCount, Tags, Status
            FROM {_qRegistry}
            WHERE AggregateType = @AggregateType AND Status = 'Active'
            ORDER BY LastEventAt DESC";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@AggregateType", aggregateType);

        var streams = new List<StreamMetadata>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
            SELECT StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                   CreatedAt, LastEventAt, EventCount, Tags, Status
            FROM {_qRegistry}
            WHERE EXISTS (SELECT 1 FROM OPENJSON(Tags) WHERE value = @Tag)
              AND Status = 'Active'
            ORDER BY LastEventAt DESC";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@Tag", tag);

        var streams = new List<StreamMetadata>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $@"
            SELECT StreamId
            FROM {_qRegistry}
            WHERE Status = 'Active'";

        if (!string.IsNullOrWhiteSpace(prefix))
        {
            sql += " AND StreamId LIKE @Prefix";
        }

        sql += " ORDER BY StreamId";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(prefix))
        {
            command.Parameters.AddWithValue("@Prefix", prefix + "%");
        }

        var streamIds = new List<string>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

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

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@AfterSequencePosition", afterSequencePosition);

        var streams = new List<StreamMetadata>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"SELECT COUNT_BIG(*) FROM {_qRegistry} WHERE Status = 'Active'";
        if (!string.IsNullOrWhiteSpace(prefix))
            sql += " AND StreamId LIKE @Prefix";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(prefix))
            command.Parameters.AddWithValue("@Prefix", prefix + "%");

        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
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
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT ISNULL(MAX(SequencePosition), 0)
            FROM {_qTable}
            """;

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is long l ? l : result is null ? 0L : Convert.ToInt64(result);
    }

    /// <inheritdoc />
    public Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _adapter.GetMaxSequencePositionAsync(
            query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

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
        var existing = await GetStreamMetadataAsync(connection, transaction, streamId, cancellationToken).ConfigureAwait(false);
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

            var command = new SqlCommand(sql, connection, transaction);
            await using var commandDisposal = command.ConfigureAwait(false);
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

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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

            var command = new SqlCommand(sql, connection, transaction);
            await using var commandDisposal = command.ConfigureAwait(false);
            command.Parameters.AddWithValue("@StreamId", streamId);
            command.Parameters.AddWithValue("@CurrentVersion", currentVersion);
            command.Parameters.AddWithValue("@LastSequencePosition", lastSequencePosition);
            command.Parameters.AddWithValue("@LastEventAt", now);
            command.Parameters.AddWithValue("@EventCountDelta", eventCountDelta);
            command.Parameters.AddWithValue("@Tags", mergedTagsJson);

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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

        var command = new SqlCommand(sql, connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@StreamId", streamId);

        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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

    private void ValidateStreamTenant(string streamId, IReadOnlyList<AppendEvent> envelopes)
    {
        var tenantIdFromStream = ExtractTenantId(streamId);
        if (_options.RequireTenantId && tenantIdFromStream == null)
        {
            throw new ArgumentException(
                $"Stream ID '{streamId}' must include tenant ID in format '{{tenantId}}:{{aggregateType}}:{{aggregateId}}' " +
                $"when RequireTenantId is true.",
                nameof(streamId));
        }

        var tenantIdFromMetadata = TryReadTenantId(envelopes);
        if (tenantIdFromStream != null && tenantIdFromMetadata != null && tenantIdFromMetadata != tenantIdFromStream)
        {
            throw new ArgumentException(
                $"Tenant ID mismatch: stream ID has '{tenantIdFromStream}' but metadata has '{tenantIdFromMetadata}'.");
        }
    }

    private static string? TryReadTenantId(IReadOnlyList<AppendEvent> envelopes)
    {
        foreach (var envelope in envelopes)
        {
            if (envelope.Metadata.IsEmpty)
                continue;

            try
            {
                using var doc = JsonDocument.Parse(envelope.Metadata);
                if (doc.RootElement.TryGetProperty("TenantId", out var property)
                    && property.ValueKind == JsonValueKind.String)
                {
                    var value = property.GetString();
                    if (!string.IsNullOrWhiteSpace(value))
                        return value;
                }
            }
            catch (JsonException)
            {
                // Metadata is opaque JSON to the log; skip unreadable blobs.
            }
        }

        return null;
    }

    private void LogConnectionOnce()
    {
        if (_logger == null || _hasLoggedConnection)
            return;

        var builder = new SqlConnectionStringBuilder(_connectionString);
        _logger.LogInformation(
            "EventStore connecting to: {DataSource}, Database: {Database}",
            builder.DataSource,
            builder.InitialCatalog);
        _hasLoggedConnection = true;
    }

    /// <summary>
    /// Copies already-serialized <see cref="AppendEvent"/> frames into the outbox
    /// in the same transaction. Does not re-serialize a CLR event.
    /// </summary>
    private async Task WriteEventsToOutboxAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyList<AppendEvent> envelopes,
        IReadOnlyList<long> sequencePositions,
        string streamId,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < envelopes.Count; i++)
        {
            var envelope = envelopes[i];
            var sequencePosition = sequencePositions[i];
            var metadataText = envelope.Metadata.IsEmpty
                ? "{}"
                : Encoding.UTF8.GetString(envelope.Metadata.Span);

            var outboxMessage = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = envelope.EventType,
                SchemaVersion = envelope.SchemaVersion,
                CodecId = envelope.CodecId,
                Payload = envelope.Payload,
                Metadata = metadataText,
                StreamId = streamId,
                SequencePosition = sequencePosition,
                CreatedAt = DateTime.UtcNow,
                Attempts = 0
            };

            var qOutbox = SqlIdentifier.Qualify(_schemaName, _options.OutboxTableName);
            var sql = $@"
                INSERT INTO {qOutbox} (
                    Id, EventType, SchemaVersion, CodecId, Payload, Metadata, CreatedAt,
                    Attempts, StreamId, SequencePosition
                )
                VALUES (
                    @Id, @EventType, @SchemaVersion, @CodecId, @Payload, @Metadata, @CreatedAt,
                    @Attempts, @StreamId, @SequencePosition
                )";

            var command = new SqlCommand(sql, connection, transaction);
            await using var commandDisposal = command.ConfigureAwait(false);
            command.Parameters.AddWithValue("@Id", outboxMessage.Id);
            command.Parameters.AddWithValue("@EventType", outboxMessage.EventType);
            command.Parameters.AddWithValue("@SchemaVersion", outboxMessage.SchemaVersion);
            command.Parameters.Add(new SqlParameter("@CodecId", SqlDbType.TinyInt) { Value = outboxMessage.CodecId });
            command.Parameters.Add(PayloadParameter("@Payload", outboxMessage.Payload));
            command.Parameters.AddWithValue("@Metadata", outboxMessage.Metadata);
            command.Parameters.AddWithValue("@CreatedAt", outboxMessage.CreatedAt);
            command.Parameters.AddWithValue("@Attempts", outboxMessage.Attempts);
            command.Parameters.AddWithValue("@StreamId", outboxMessage.StreamId);
            command.Parameters.AddWithValue("@SequencePosition", outboxMessage.SequencePosition);

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        if (_logger is not null)
            SqlServerEventStoreLog.OutboxEventsWritten(_logger, envelopes.Count, streamId);
    }

    /// <inheritdoc />
    public ISubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default)
        => _adapter.Subscribe(subscriberId, fromSequence, filter, cancellationToken);

    /// <inheritdoc />
    IEventLogSubscriptionHandle IEventLogSubscriptions.Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter,
        CancellationToken cancellationToken)
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

                await foreach (var frame in ReadRecordedQueryStreamAsync(readQuery, fromSequencePosition: from, cancellationToken: ct)
                    .ConfigureAwait(false))
                {
                    if (ct.IsCancellationRequested || handle.IsDisposed)
                        break;

                    if (frame.SequencePosition <= lastDelivered)
                        continue;

                    if (!handle.Filter.Matches(frame))
                        continue;

                    await handle.WriteAsync(frame, ct).ConfigureAwait(false);
                    lastDelivered = frame.SequencePosition;
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
            if (_logger is not null)
                SqlServerEventStoreLog.SubscriptionFailed(_logger, ex, handle.SubscriberId);
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

    async Task<IReadOnlyList<RecordedEvent>> IEventLog.ReadStreamAsync(
        string streamId,
        long fromVersion,
        long? toVersion,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return await ReadRecordedStreamCoreAsync(
            connection, streamId, fromVersion, toVersion, toCommitTimestamp, cancellationToken)
            .ConfigureAwait(false);
    }

    async IAsyncEnumerable<RecordedEvent> IEventLog.ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion,
        long? toVersion,
        DateTime? toCommitTimestamp,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        foreach (var frame in await ReadRecordedStreamCoreAsync(
            connection, streamId, fromVersion, toVersion, toCommitTimestamp, cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return frame;
        }
    }

    async Task<EventLogQueryResult> IEventLog.ReadByQueryAsync(
        Query query,
        long? fromSequencePosition,
        int? limit,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var frames = await ReadRecordedQueryCoreAsync(
            query, fromSequencePosition, limit, toSequencePosition, toCommitTimestamp, cancellationToken)
            .ConfigureAwait(false);
        return new EventLogQueryResult(frames);
    }

    IAsyncEnumerable<RecordedEvent> IEventLog.ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        return ReadRecordedQueryStreamAsync(
            query, fromSequencePosition, toSequencePosition, toCommitTimestamp, cancellationToken);
    }

    Task<long> IEventLog.GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        return GetMaxSequencePositionCoreAsync(
            query, fromSequencePosition, toSequencePosition, toCommitTimestamp, cancellationToken);
    }

    public async Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<AppendEvent> events,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        var envelopes = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (envelopes.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));

        ValidateStreamTenant(streamId, envelopes);
        LogConnectionOnce();

        return await ExecuteWithRetryAsync(async ct =>
        {
            await EnsureEventTypeIdsAsync(envelopes, ct).ConfigureAwait(false);

            var connection = new SqlConnection(_connectionString);
            await using var connectionDisposal = connection.ConfigureAwait(false);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            var transaction = connection.BeginTransaction();
            await using var transactionDisposal = transaction.ConfigureAwait(false);

            try
            {
                if (expectedVersion.HasValue)
                {
                    var currentVersion = await GetCurrentVersionAsync(connection, transaction, streamId, ct).ConfigureAwait(false);
                    if (currentVersion != expectedVersion.Value)
                    {
                        throw new ConcurrencyException(
                            $"Expected version {expectedVersion.Value} but current version is {currentVersion}",
                            expectedVersion.Value,
                            currentVersion);
                    }
                }

                var streamCurrentVersion = await GetCurrentVersionAsync(connection, transaction, streamId, ct).ConfigureAwait(false);
                var startingVersion = streamCurrentVersion < 0 ? 1 : streamCurrentVersion + 1;
                var sequencePositions = await InsertEventsBatchAsync(
                    connection, transaction, streamId, startingVersion, envelopes, ct).ConfigureAwait(false);

                if (_enableRegistry)
                {
                    await UpsertStreamMetadataAsync(
                        connection,
                        transaction,
                        streamId,
                        startingVersion + envelopes.Count - 1,
                        sequencePositions.Last(),
                        envelopes.Count,
                        UnionTags(envelopes),
                        ct).ConfigureAwait(false);
                }

                if (_options.EnableOutbox)
                {
                    await WriteEventsToOutboxAsync(
                        connection, transaction, envelopes, sequencePositions, streamId, ct)
                        .ConfigureAwait(false);
                }

                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new AppendResult(sequencePositions, null, startingVersion + envelopes.Count - 1);
            }
            catch
            {
                if (transaction.Connection != null)
                {
                    try { await transaction.RollbackAsync(ct).ConfigureAwait(false); }
                    catch { /* already rolled back */ }
                }
                throw;
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<AppendResult> AppendAsync(
        IEnumerable<AppendEvent> events,
        AppendCondition condition,
        CancellationToken cancellationToken = default)
    {
        var envelopes = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (envelopes.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));
        ArgumentNullException.ThrowIfNull(condition);

        LogConnectionOnce();

        return await ExecuteWithRetryAsync(async ct =>
        {
            await EnsureEventTypeIdsAsync(envelopes, ct).ConfigureAwait(false);

            var connection = new SqlConnection(_connectionString);
            await using var connectionDisposal = connection.ConfigureAwait(false);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            var transaction = connection.BeginTransaction();
            await using var transactionDisposal = transaction.ConfigureAwait(false);

            try
            {
                await AcquireDcbFenceLocksAsync(connection, transaction, condition, ct).ConfigureAwait(false);

                if (await AppendConditionMatchesAsync(connection, transaction, condition, ct).ConfigureAwait(false))
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    throw new ConcurrencyException(
                        "Append condition failed: matching events exist",
                        condition.After);
                }

                var tenantId = TryReadTenantId(envelopes);
                if (_options.RequireTenantId && string.IsNullOrWhiteSpace(tenantId))
                {
                    throw new ArgumentException(
                        "TenantId is required in event metadata when RequireTenantId is true for DCB appends.");
                }

                var streamId = !string.IsNullOrWhiteSpace(tenantId)
                    ? $"{tenantId}:dcb:{Guid.NewGuid()}"
                    : $"dcb:{Guid.NewGuid()}";
                var sequencePositions = await InsertEventsBatchAsync(
                    connection, transaction, streamId, 0, envelopes, ct).ConfigureAwait(false);

                if (_enableRegistry)
                {
                    await UpsertStreamMetadataAsync(
                        connection,
                        transaction,
                        streamId,
                        0,
                        sequencePositions.Last(),
                        envelopes.Count,
                        UnionTags(envelopes),
                        ct).ConfigureAwait(false);
                }

                if (_options.EnableOutbox)
                {
                    await WriteEventsToOutboxAsync(
                        connection, transaction, envelopes, sequencePositions, streamId, ct)
                        .ConfigureAwait(false);
                }

                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new AppendResult(sequencePositions);
            }
            catch
            {
                if (transaction.Connection != null)
                {
                    try { await transaction.RollbackAsync(ct).ConfigureAwait(false); }
                    catch { /* already rolled back */ }
                }
                throw;
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<RecordedEvent>> ReadRecordedStreamCoreAsync(
        SqlConnection connection,
        string streamId,
        long fromVersion,
        long? toVersion,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        var sql = $@"
            SELECT {StreamEventColumns}
            FROM {_qTable}
            WHERE StreamId = @StreamId AND Version >= @FromVersion";

        if (toVersion.HasValue)
            sql += " AND Version <= @ToVersion";
        if (toCommitTimestamp.HasValue)
            sql += " AND Timestamp <= @ToTimestamp";

        sql += " ORDER BY Version";

        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@StreamId", streamId);
        command.Parameters.AddWithValue("@FromVersion", fromVersion);
        if (toVersion.HasValue)
            command.Parameters.AddWithValue("@ToVersion", toVersion.Value);
        if (toCommitTimestamp.HasValue)
            command.Parameters.AddWithValue("@ToTimestamp", toCommitTimestamp.Value);

        var rows = new List<EventRow>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                rows.Add(ReadEventRow(reader));
        }

        return await MaterializeAsync(rows, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<RecordedEvent>> ReadRecordedQueryCoreAsync(
        Query query,
        long? fromSequencePosition,
        int? limit,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        var built = DcbQuerySql.Build(
            _qTable,
            _qTags,
            query,
            _eventTypes.FindId,
            fromSequencePosition,
            toSequencePosition,
            toCommitTimestamp,
            limit,
            DcbQuerySql.Mode.Events);

        if (built.ShortCircuited)
            return [];

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var command = new SqlCommand(built.Sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddRange(built.Parameters.ToArray());

        var rows = new List<EventRow>();
        var reader = (await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false));
        await using (reader.ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                rows.Add(ReadEventRow(reader));
        }

        return await MaterializeAsync(rows, cancellationToken).ConfigureAwait(false);
    }

    private async IAsyncEnumerable<RecordedEvent> ReadRecordedQueryStreamAsync(
        Query query,
        long? fromSequencePosition,
        long? toSequencePosition = null,
        DateTime? toCommitTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var frames = await ReadRecordedQueryCoreAsync(
            query, fromSequencePosition, limit: null, toSequencePosition, toCommitTimestamp, cancellationToken)
            .ConfigureAwait(false);

        foreach (var frame in frames)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return frame;
        }
    }

    private async Task<long> GetMaxSequencePositionCoreAsync(
        Query query,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        var built = DcbQuerySql.Build(
            _qTable,
            _qTags,
            query,
            _eventTypes.FindId,
            fromSequencePosition,
            toSequencePosition,
            toCommitTimestamp,
            limit: null,
            DcbQuerySql.Mode.MaxSequence);

        if (built.ShortCircuited)
            return 0L;

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var command = new SqlCommand(built.Sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddRange(built.Parameters.ToArray());
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is long l ? l : result is null || result is DBNull ? 0L : Convert.ToInt64(result);
    }
}

