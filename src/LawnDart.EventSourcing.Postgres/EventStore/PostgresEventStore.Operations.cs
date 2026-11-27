using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.Json;
using LawnDart.EventStore;
using Npgsql;
using NpgsqlTypes;

namespace LawnDart.EventSourcing.Postgres.EventStore;

public sealed partial class PostgresEventStore
{
    private readonly record struct AppendWrite(IReadOnlyList<long> Positions, long LastVersion);

    private readonly record struct Stamp(int Format, bool Partitioning, long? PartitionSize);

    /// <summary>
    /// Fixed for every attempt of one append. Every attempt writes the same time,
    /// so a read-back after a closed connection finds the rows an earlier attempt saved.
    /// Every position this append can get is above <see cref="Floor"/>, the newest
    /// position this store had seen before the first attempt.
    /// The time is taken when the first attempt builds its statement, as late as it
    /// can be while still being the same for every attempt.
    /// </summary>
    private sealed class AppendStamp(long floor, DateTime? recordedAt = null)
    {
        private DateTime? _recordedAt = recordedAt;

        public long Floor { get; } = floor;

        /// <summary>True once an attempt took the time. Before that nothing was sent.</summary>
        public bool IsFixed => _recordedAt.HasValue;

        /// <summary>The time every attempt writes. Only valid after <see cref="Fix"/>.</summary>
        public DateTime RecordedAt => _recordedAt ?? throw new InvalidOperationException("The append time was not taken yet.");

        /// <summary>
        /// Takes the time on the first call and returns the same value after that. It is cut
        /// to whole microseconds, which is what Postgres stores, so a lookup matches it exactly.
        /// </summary>
        public DateTime Fix()
        {
            if (_recordedAt is { } fixedAt)
                return fixedAt;
            var now = DateTime.UtcNow;
            var recorded = new DateTime(now.Ticks - (now.Ticks % 10), DateTimeKind.Utc);
            _recordedAt = recorded;
            return recorded;
        }
    }

    internal static NpgsqlDataSource Source(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            IncludeErrorDetail = true
        };
        return Sources.GetOrAdd(builder.ConnectionString, static value => new NpgsqlDataSourceBuilder(value).Build());
    }

    private async Task<AppendResult> AppendWithRetry(
        Func<CancellationToken, Task<AppendResult>> action,
        string streamId,
        IReadOnlyList<Guid> eventIds,
        IReadOnlyList<string> eventTypes,
        bool isDcb,
        AppendStamp stamp,
        CancellationToken cancellationToken)
    {
        // Retry a closed connection only when no result row was read. Postgres
        // commits an autocommit statement when it handles the end of the request,
        // after the rows are sent, so a closure after a row can follow a real
        // commit. A transaction pooler can report a closure itself, after the
        // commit and before any row. Event ids are not unique, so a second try
        // would not fail on its own: read this append back first and return it
        // when it was saved. A first try that commits after that read is saved
        // twice unless the append has an expected version or a condition, which
        // the second try then fails. The second try reuses these event ids and
        // this stamp. If that try looks like a conflict, read the ids back again
        // before telling the caller the append failed.
        var max = _options.MaxRetryCount;
        var closedConnectionRetried = false;
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await action(cancellationToken).ConfigureAwait(false);
            }
            catch (PostgresException ex) when (attempt < max && ex.SqlState is "40001" or "40P01")
            {
                await DelayRetryAsync(attempt, ex.SqlState, ex.MessageText, cancellationToken).ConfigureAwait(false);
            }
            catch (PostgresException ex) when (attempt < max && IsRangeAttachedMeanwhile(ex))
            {
                // Defensive. If an insert ever uses a list of ranges read before another
                // session attached the range it needs, it fails here having written
                // nothing, and a new transaction reads the current list. No test reaches
                // this path today; the filter has its own unit test.
                await DelayRetryAsync(attempt, ex.SqlState, ex.MessageText, cancellationToken).ConfigureAwait(false);
            }
            catch (ClosedConnectionBeforeCommitException ex) when (!closedConnectionRetried && !cancellationToken.IsCancellationRequested)
            {
                closedConnectionRetried = true;
                // COMMIT was not sent, so the attempt cannot commit.
                await PrepareClosedConnectionRetryAsync(ex.InnerException ?? ex, cancellationToken).ConfigureAwait(false);
                attempt--;
            }
            catch (ClosedConnectionBeforeCommitException ex)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException ?? ex).Throw();
            }
            catch (Exception ex) when (!closedConnectionRetried
                && !cancellationToken.IsCancellationRequested
                && PostgresConnectionClosure.HasRetryableClosureSqlState(ex))
            {
                closedConnectionRetried = true;
                // An autocommit attempt can still commit after the closure was reported.
                await PrepareClosedConnectionRetryAsync(ex, cancellationToken).ConfigureAwait(false);
                var read = await ReadBackOrUnknownAsync(streamId, eventIds, eventTypes, stamp, cancellationToken).ConfigureAwait(false);
                if (read.Kind is AppendReadKind.Match or AppendReadKind.Partial)
                    return ResolveReadBack(read, isDcb, ex, missingIsUnknown: true);
                attempt--;
            }
            catch (Exception ex) when (closedConnectionRetried && IsConflictAfterRetry(ex))
            {
                return await ReturnSavedOrRethrowAsync(streamId, eventIds, eventTypes, isDcb, stamp, ex, cancellationToken).ConfigureAwait(false);
            }
            catch (NpgsqlException ex) when (ex is not PostgresException
                && attempt < max
                && IsConnectionFailure(ex)
                && !PostgresConnectionClosure.HasRetryableClosureSqlState(ex))
            {
                // Only a failure before the statement is sent. A failure after
                // ExecuteReader is AppendStatementSentException and is not retried.
                await DelayRetryAsync(attempt, ex.SqlState ?? "08", ex.Message, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// "No partition of relation ... found for row" on the events or tag table. A check
    /// constraint failure has the same code but names its constraint.
    /// </summary>
    private bool IsRangeAttachedMeanwhile(PostgresException exception)
        => IsRangeAttachedMeanwhile(exception, _options.UsesPositionPartitions, _options.EventsTableName, _options.EventTagsTableName);

    /// <summary>
    /// True only for "no partition of relation found for row" (23514 with no constraint
    /// name) on the events or event tags table while position ranges are on. That error is
    /// raised during the statement, so the try wrote nothing and a new transaction is safe.
    /// </summary>
    internal static bool IsRangeAttachedMeanwhile(PostgresException exception, bool ranges, string eventsTable, string tagsTable)
        => ranges
            && exception.SqlState == "23514"
            && exception.ConstraintName is null
            && (string.Equals(exception.TableName, eventsTable, StringComparison.Ordinal)
                || string.Equals(exception.TableName, tagsTable, StringComparison.Ordinal));

    private static bool IsConflictAfterRetry(Exception exception)
        => exception is ConcurrencyException or EventAlreadyAppendedException;

    private async Task<AppendResult> ReturnSavedOrRethrowAsync(
        string streamId,
        IReadOnlyList<Guid> eventIds,
        IReadOnlyList<string> eventTypes,
        bool isDcb,
        AppendStamp stamp,
        Exception conflict,
        CancellationToken cancellationToken)
    {
        var read = await ReadBackOrUnknownAsync(streamId, eventIds, eventTypes, stamp, cancellationToken).ConfigureAwait(false);
        return ResolveReadBack(read, isDcb, conflict, missingIsUnknown: false);
    }

    private async Task PrepareClosedConnectionRetryAsync(Exception exception, CancellationToken cancellationToken)
    {
        // This data source is cached for the connection string. Clear drops every
        // idle connection on it, including idle connections of another store that
        // uses the same string. Npgsql discards a connector that already failed
        // when that connector is closed. It does not offer a way to drop one
        // other idle connector, so those idle connections are cleared together.
        _dataSource.Clear();
        if (_logger is not null)
            PostgresEventStoreLog.AppendConnectionClosed(_logger, exception);
        if (PostgresConnectionClosure.IsDatabaseStarting(exception))
            await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
    }

    private async Task DelayRetryAsync(int attempt, string code, string message, CancellationToken cancellationToken)
    {
        var delay = TimeSpan.FromMilliseconds(_options.RetryDelay.TotalMilliseconds * (attempt + 1));
        if (_logger is not null)
            PostgresEventStoreLog.TransientError(_logger, attempt + 1, _options.MaxRetryCount, code, message, delay.TotalMilliseconds);
        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsConnectionFailure(NpgsqlException exception)
    {
        var state = exception.SqlState;
        return state is null || state.StartsWith("08", StringComparison.Ordinal);
    }

    private async Task<AppendWrite> ExecuteAppendAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string streamId,
        IReadOnlyList<AppendEvent> envelopes,
        IReadOnlyList<Guid> eventIds,
        long? expectedVersion,
        AppendCondition? condition,
        string? conditionTenant,
        bool isDcb,
        AppendStamp stamp,
        CancellationToken cancellationToken)
    {
        var tenant = TenantKey.ForStream(streamId, _options.Tenancy, _options.SingleTenantId);
        var payloads = new string?[envelopes.Count];
        var binaries = new byte[]?[envelopes.Count];
        var metadata = new string?[envelopes.Count];
        var typeIds = new int[envelopes.Count];
        var schemaVersions = new int[envelopes.Count];
        var codecs = new short[envelopes.Count];
        var tagOrds = new List<int>();
        var tagValues = new List<string>();
        var streamTags = new HashSet<string>(StringComparer.Ordinal);
        var eventTags = new HashSet<Tag>();

        for (var i = 0; i < envelopes.Count; i++)
        {
            var envelope = envelopes[i];
            var typeId = _eventTypes.FindId(envelope.EventType)
                ?? throw new InvalidOperationException($"Event name '{envelope.EventType}' is not in {_names.Types}.");
            typeIds[i] = typeId;
            schemaVersions[i] = envelope.SchemaVersion;
            codecs[i] = envelope.CodecId;
            var (json, binary) = SplitPayload(envelope.CodecId, envelope.Payload);
            payloads[i] = json;
            binaries[i] = binary;
            metadata[i] = MetadataJson(envelope.Metadata);
            eventTags.Clear();
            foreach (var tag in envelope.Tags)
            {
                if (tag.IsDefault || !eventTags.Add(tag))
                    continue;
                tagOrds.Add(i + 1);
                tagValues.Add(tag.ToString());
                streamTags.Add(tag.ToString());
            }
        }

        BuildLocks(envelopes, condition, out var lockTags, out var lockModes, out var lockStore);
        BuildCondition(condition, out var check, out var all, out var after, out var itemCount, out var typeItems, out var typeCond, out var tagItems, out var tagCond, out var itemsWithTypes, out var itemsWithTags);
        // The stored function returns one row per event, in batch order. The outbox
        // insert stays in this statement and joins those rows by batch position, so
        // only envelopes that carry Outbound get a row. The function body and the
        // schema stamp stay as they are. A batch with no outbound value uses the
        // plain append statement.
        var outboxSlots = new List<int>();
        if (_options.EnableOutbox)
        {
            for (var i = 0; i < envelopes.Count; i++)
            {
                if (envelopes[i].Outbound is not null)
                    outboxSlots.Add(i);
            }
        }

        var writeOutbox = outboxSlots.Count > 0;
        int[]? outboxOrds = null;
        Guid[]? outboxEventIds = null;
        string[]? eventTypeNames = null;
        int[]? outboxSchemaVersions = null;
        short[]? outboxCodecs = null;
        byte[][]? outboxPayloads = null;
        string?[]? outboxMetadata = null;
        if (writeOutbox)
        {
            var count = outboxSlots.Count;
            outboxOrds = new int[count];
            outboxEventIds = new Guid[count];
            eventTypeNames = new string[count];
            outboxSchemaVersions = new int[count];
            outboxCodecs = new short[count];
            outboxPayloads = new byte[count][];
            outboxMetadata = new string?[count];
            for (var slot = 0; slot < count; slot++)
            {
                var index = outboxSlots[slot];
                var outbound = envelopes[index].Outbound!;
                outboxOrds[slot] = index + 1;
                outboxEventIds[slot] = eventIds[index];
                eventTypeNames[slot] = outbound.EventType;
                outboxSchemaVersions[slot] = outbound.SchemaVersion;
                outboxCodecs[slot] = outbound.CodecId;
                outboxPayloads[slot] = outbound.Payload.IsEmpty ? [] : outbound.Payload.ToArray();
                outboxMetadata[slot] = MetadataJson(outbound.Metadata);
            }
        }

        const string emptyJson = "'{}'::jsonb";

        var call = $"""
            {_names.Append}(
                @stream_id, @tenant_id, @expected, @is_dcb, @aggregate_type, @aggregate_id,
                @event_ids, @type_ids, @schema_versions, @codec_ids, @data, @bdata, @metadata,
                @tag_ords, @tag_values, @stream_tags, @partition_hash, @recorded_at, @notify, @notify_channel,
                @lock_tags, @lock_modes, @lock_store, @store_lock_key,
                @check_condition, @condition_all, @condition_tenant, @condition_after, @item_count,
                @type_items, @type_ids_cond, @tag_items, @tag_values_cond, @items_with_types, @items_with_tags, @partition_limit)
            """;
        // One statement. MATERIALIZED runs the original function once. The outbox
        // insert is in that same statement, so a rollback removes both. An older
        // init that recreates only this function still serves the call.
        var sql = writeOutbox
            ? $"""
            WITH r AS MATERIALIZED (
                SELECT src.global_position, src.version, src.ord
                FROM {call} WITH ORDINALITY AS src(global_position, version, ord)
            ),
            written AS (
                INSERT INTO {_names.Outbox} (
                    id, event_type, schema_version, codec_id, payload, metadata,
                    created_at, attempts, stream_id, sequence_position)
                SELECT
                    u.event_id,
                    u.event_type,
                    u.schema_version,
                    u.codec_id,
                    u.payload,
                    coalesce(u.metadata, {emptyJson}),
                    @recorded_at,
                    0,
                    @stream_id,
                    r.global_position
                FROM r
                JOIN unnest(
                    @outbox_ords, @outbox_event_ids, @event_type_names, @outbox_schema_versions,
                    @outbox_codec_ids, @outbox_payloads, @outbox_metadata
                ) AS u(ord, event_id, event_type, schema_version, codec_id, payload, metadata)
                    ON u.ord = r.ord
                RETURNING sequence_position
            )
            SELECT r.global_position, r.version
            FROM r
            WHERE (SELECT count(*) FROM written) >= 0
            ORDER BY r.ord
            """
            : $"""
            SELECT global_position, version
            FROM {call}
            """;
        var command = new NpgsqlCommand(sql, connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.Add(new NpgsqlParameter("stream_id", streamId));
        command.Parameters.Add(new NpgsqlParameter("tenant_id", tenant));
        command.Parameters.Add(new NpgsqlParameter("expected", NpgsqlDbType.Bigint) { Value = (object?)expectedVersion ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("is_dcb", isDcb));
        command.Parameters.Add(new NpgsqlParameter("aggregate_type", StreamIdText.AggregateType(streamId)));
        var aggregateId = StreamIdText.AggregateId(streamId);
        command.Parameters.Add(new NpgsqlParameter("aggregate_id", NpgsqlDbType.Uuid) { Value = (object?)aggregateId ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("event_ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = eventIds.ToArray() });
        command.Parameters.Add(new NpgsqlParameter("type_ids", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = typeIds });
        command.Parameters.Add(new NpgsqlParameter("schema_versions", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = schemaVersions });
        command.Parameters.Add(new NpgsqlParameter("codec_ids", NpgsqlDbType.Array | NpgsqlDbType.Smallint) { Value = codecs });
        command.Parameters.Add(new NpgsqlParameter("data", NpgsqlDbType.Array | NpgsqlDbType.Jsonb) { Value = payloads });
        command.Parameters.Add(new NpgsqlParameter("bdata", NpgsqlDbType.Array | NpgsqlDbType.Bytea) { Value = binaries });
        command.Parameters.Add(new NpgsqlParameter("metadata", NpgsqlDbType.Array | NpgsqlDbType.Jsonb) { Value = metadata });
        command.Parameters.Add(new NpgsqlParameter("tag_ords", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = tagOrds.ToArray() });
        command.Parameters.Add(new NpgsqlParameter("tag_values", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = tagValues.ToArray() });
        command.Parameters.Add(new NpgsqlParameter("stream_tags", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = streamTags.ToArray() });
        command.Parameters.Add(new NpgsqlParameter("partition_hash", PartitionHashUtility.GetDeterministicHashCode(streamId)));
        command.Parameters.Add(new NpgsqlParameter("recorded_at", NpgsqlDbType.TimestampTz) { Value = stamp.Fix() });
        command.Parameters.Add(new NpgsqlParameter("notify", _options.NotifyOnAppend));
        command.Parameters.Add(new NpgsqlParameter("notify_channel", _names.NotifyChannel));
        command.Parameters.Add(new NpgsqlParameter("lock_tags", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = lockTags });
        command.Parameters.Add(new NpgsqlParameter("lock_modes", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = lockModes });
        command.Parameters.Add(new NpgsqlParameter("lock_store", lockStore));
        command.Parameters.Add(new NpgsqlParameter("store_lock_key", _names.Schema + ":store"));
        command.Parameters.Add(new NpgsqlParameter("check_condition", check));
        command.Parameters.Add(new NpgsqlParameter("condition_all", all));
        command.Parameters.Add(new NpgsqlParameter("condition_tenant", NpgsqlDbType.Text) { Value = (object?)conditionTenant ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("condition_after", NpgsqlDbType.Bigint) { Value = (object?)after ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("item_count", itemCount));
        command.Parameters.Add(new NpgsqlParameter("type_items", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = typeItems });
        command.Parameters.Add(new NpgsqlParameter("type_ids_cond", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = typeCond });
        command.Parameters.Add(new NpgsqlParameter("tag_items", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = tagItems });
        command.Parameters.Add(new NpgsqlParameter("tag_values_cond", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = tagCond });
        command.Parameters.Add(new NpgsqlParameter("items_with_types", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = itemsWithTypes });
        command.Parameters.Add(new NpgsqlParameter("items_with_tags", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = itemsWithTags });
        command.Parameters.Add(new NpgsqlParameter("partition_limit", NpgsqlDbType.Bigint) { Value = PartitionLimitParameter() });
        if (writeOutbox)
        {
            command.Parameters.Add(new NpgsqlParameter("outbox_ords", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = outboxOrds });
            command.Parameters.Add(new NpgsqlParameter("outbox_event_ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = outboxEventIds });
            command.Parameters.Add(new NpgsqlParameter("event_type_names", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = eventTypeNames });
            command.Parameters.Add(new NpgsqlParameter("outbox_schema_versions", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = outboxSchemaVersions });
            command.Parameters.Add(new NpgsqlParameter("outbox_codec_ids", NpgsqlDbType.Array | NpgsqlDbType.Smallint) { Value = outboxCodecs });
            command.Parameters.Add(new NpgsqlParameter("outbox_payloads", NpgsqlDbType.Array | NpgsqlDbType.Bytea) { Value = outboxPayloads });
            command.Parameters.Add(new NpgsqlParameter("outbox_metadata", NpgsqlDbType.Array | NpgsqlDbType.Jsonb) { Value = outboxMetadata });
        }

        // A closure before any result row means this call did not observe a saved
        // append, so it may be tried once. Postgres commits an autocommit statement
        // when it handles the end of the request, after the rows are sent. A
        // closure after a row can follow that commit, including when a transaction
        // pooler reports the drop. Read the event ids back instead of trying again.
        var rowsRead = 0;
        try
        {
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using var readerDisposal = reader.ConfigureAwait(false);
            try
            {
                var positions = new List<long>(envelopes.Count);
                long lastVersion = -1;
                if (AfterAppendStatement is { } afterStatement)
                    await afterStatement(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    rowsRead++;
                    positions.Add(reader.GetInt64(0));
                    lastVersion = reader.GetInt64(1);
                }

                return new AppendWrite(positions, lastVersion);
            }
            catch (OperationCanceledException ex)
            {
                // ExecuteReader already returned, so the statement was sent.
                throw new AppendStatementSentException(ex);
            }
        }
        catch (PostgresException ex) when (ex.SqlState is "40001" or "40P01")
        {
            throw;
        }
        catch (PostgresException ex) when (ex.SqlState == "08007" || (rowsRead > 0 && PostgresConnectionClosure.HasRetryableClosureSqlState(ex)))
        {
            // Commit outcome cannot be told, or a row was already read.
            // Read the event ids back. Do not retry.
            throw new AppendStatementSentException(ex);
        }
        catch (PostgresException ex) when (PostgresConnectionClosure.HasRetryableClosureSqlState(ex))
        {
            throw;
        }
        catch (PostgresException ex)
        {
            throw MapAppendError(ex, expectedVersion, condition?.After, _options.OutboxTableName);
        }
        catch (AppendStatementSentException)
        {
            throw;
        }
        catch (ClosedConnectionBeforeCommitException)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            && rowsRead == 0
            && transaction is not null
            && (PostgresConnectionClosure.HasTransportBreak(ex) || PostgresConnectionClosure.IsConnectorBroken(connection)))
        {
            // BEGIN is sent with this call. COMMIT is a later round trip.
            // No result row was read, so the commit was not sent.
            throw new ClosedConnectionBeforeCommitException(ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The statement was submitted. An autocommit call may already be committed.
            throw new AppendStatementSentException(ex);
        }
    }

    private static Exception MapAppendError(
        PostgresException exception,
        long? expectedVersion,
        long? conditionAfter,
        string outboxTable)
    {
        if (exception.SqlState == "23505"
            && OutboxKeyViolation(exception, outboxTable)
            && TryReadDuplicateKey(exception.Detail, out var duplicateId))
            return new EventAlreadyAppendedException(duplicateId);

        if (exception.MessageText == "condition")
        {
            return new ConcurrencyException(
                "Append condition failed: matching events exist",
                exception,
                conditionAfter);
        }

        if (exception.Hint == "concurrency")
        {
            var (expected, actual) = ParseVersionDetail(exception.Detail, expectedVersion);
            return new ConcurrencyException(
                $"Expected version {expected} but current version is {actual}",
                exception,
                expected,
                actual);
        }

        return exception;
    }

    private static (long? Expected, long? Actual) ParseVersionDetail(string? detail, long? expectedVersion)
    {
        if (string.IsNullOrEmpty(detail))
            return (expectedVersion, null);

        var colon = detail.IndexOf(':');
        if (colon < 0)
        {
            long? only = long.TryParse(detail, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : null;
            return (expectedVersion ?? only, only);
        }

        long? expected = long.TryParse(detail[..colon], NumberStyles.Integer, CultureInfo.InvariantCulture, out var left)
            ? left
            : expectedVersion;
        long? actual = long.TryParse(detail[(colon + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var right)
            ? right
            : null;
        return (expected, actual);
    }

    private static bool OutboxKeyViolation(PostgresException exception, string outboxTable)
        => string.Equals(exception.TableName, outboxTable, StringComparison.Ordinal)
           || string.Equals(exception.ConstraintName, outboxTable + "_pkey", StringComparison.Ordinal);

    private static bool TryReadDuplicateKey(string? detail, out Guid eventId)
    {
        eventId = default;
        if (string.IsNullOrEmpty(detail))
            return false;

        var open = detail.LastIndexOf('(');
        var close = detail.LastIndexOf(')');
        if (open < 0 || close <= open)
            return false;

        return Guid.TryParse(detail[(open + 1)..close], out eventId);
    }

    private void BuildLocks(
        IReadOnlyList<AppendEvent> envelopes,
        AppendCondition? condition,
        out string[] lockTags,
        out string[] lockModes,
        out string lockStore)
    {
        var exclusive = new HashSet<string>(StringComparer.Ordinal);
        var shared = new HashSet<string>(StringComparer.Ordinal);
        if (condition is not null)
        {
            foreach (var tag in CollectFenceTags(condition))
                exclusive.Add(tag);
        }

        foreach (var tag in envelopes.SelectMany(envelope => envelope.Tags))
        {
            if (!tag.IsDefault && !exclusive.Contains(tag.ToString()))
                shared.Add(tag.ToString());
        }

        if (exclusive.Count + shared.Count > _options.AdvisoryLockTagThreshold)
        {
            lockTags = [];
            lockModes = [];
            lockStore = "exclusive";
            return;
        }

        var ordered = new List<(string Tag, string Mode)>(exclusive.Count + shared.Count);
        foreach (var tag in exclusive)
            ordered.Add((tag, "exclusive"));
        foreach (var tag in shared)
            ordered.Add((tag, "shared"));
        ordered.Sort(static (left, right) => string.CompareOrdinal(left.Tag, right.Tag));
        lockTags = ordered.Select(static item => item.Tag).ToArray();
        lockModes = ordered.Select(static item => item.Mode).ToArray();
        // Any condition item with no tags can match events the tag locks do not cover.
        // That append takes the exclusive store lock, even when another item has tags.
        lockStore = condition is not null && ConditionNeedsStoreExclusive(condition) ? "exclusive" : "shared";
    }

    private static bool ConditionNeedsStoreExclusive(AppendCondition condition)
    {
        if (condition.FailIfEventsMatch.Items.Count == 0)
            return true;

        foreach (var item in condition.FailIfEventsMatch.Items)
        {
            if (item.Tags is not { Count: > 0 } || item.Tags.All(static t => t.IsDefault))
                return true;
        }

        return false;
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
                if (!tag.IsDefault)
                    tags.Add(tag.ToString());
            }
        }

        var sorted = tags.ToList();
        sorted.Sort(StringComparer.Ordinal);
        return sorted;
    }

    private void BuildCondition(
        AppendCondition? condition,
        out bool check,
        out bool all,
        out long? after,
        out int itemCount,
        out int[] typeItems,
        out int[] typeCond,
        out int[] tagItems,
        out string[] tagCond,
        out int[] itemsWithTypes,
        out int[] itemsWithTags)
    {
        typeItems = [];
        typeCond = [];
        tagItems = [];
        tagCond = [];
        itemsWithTypes = [];
        itemsWithTags = [];
        itemCount = 0;
        after = condition?.After;
        if (condition is null)
        {
            check = false;
            all = false;
            return;
        }

        check = true;
        if (condition.FailIfEventsMatch.Items.Count == 0)
        {
            all = true;
            return;
        }

        all = false;
        var typesI = new List<int>();
        var typesId = new List<int>();
        var tagsI = new List<int>();
        var tagsV = new List<string>();
        var withTypes = new List<int>();
        var withTags = new List<int>();
        var number = 0;
        foreach (var item in condition.FailIfEventsMatch.Items)
        {
            int[]? ids = null;
            if (item.Types is { Count: > 0 })
            {
                var found = new List<int>();
                foreach (var token in item.Types)
                {
                    if (_eventTypes.FindId(token) is int id)
                        found.Add(id);
                }

                if (found.Count == 0)
                    continue;
                ids = found.ToArray();
            }

            number++;
            if (ids is not null)
            {
                withTypes.Add(number);
                foreach (var id in ids)
                {
                    typesI.Add(number);
                    typesId.Add(id);
                }
            }

            if (item.Tags is { Count: > 0 })
            {
                withTags.Add(number);
                foreach (var tag in item.Tags)
                {
                    if (tag.IsDefault)
                        continue;
                    tagsI.Add(number);
                    tagsV.Add(tag.ToString());
                }
            }
        }

        itemCount = number;
        typeItems = typesI.ToArray();
        typeCond = typesId.ToArray();
        tagItems = tagsI.ToArray();
        tagCond = tagsV.ToArray();
        itemsWithTypes = withTypes.ToArray();
        itemsWithTags = withTags.ToArray();
        if (itemCount == 0)
            check = false;
    }

    private static (string? Json, byte[]? Binary) SplitPayload(byte codecId, ReadOnlyMemory<byte> payload)
    {
        if (codecId != EventCodec.Json || payload.IsEmpty)
            return (null, payload.IsEmpty ? [] : payload.ToArray());

        var text = Encoding.UTF8.GetString(payload.Span);
        if (text.Contains('\0', StringComparison.Ordinal) || text.Contains("\\u0000", StringComparison.OrdinalIgnoreCase))
            return (null, payload.ToArray());

        try
        {
            using var document = JsonDocument.Parse(text);
            return (text, null);
        }
        catch (JsonException)
        {
            return (null, payload.ToArray());
        }
    }

    private static string? MetadataJson(ReadOnlyMemory<byte> metadata)
    {
        if (metadata.IsEmpty)
            return null;

        var text = Encoding.UTF8.GetString(metadata.Span);
        if (text.Contains('\0', StringComparison.Ordinal) || text.Contains("\\u0000", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Event metadata JSON cannot contain a null character.");

        try
        {
            using var document = JsonDocument.Parse(text);
            return text;
        }
        catch (JsonException ex)
        {
            throw new ArgumentException("Event metadata must be JSON.", ex);
        }
    }

    private void Signal(AppendResult result)
    {
        long newest = -1;
        foreach (var position in result.SequencePositions)
        {
            if (position > newest)
                newest = position;
        }

        if (newest >= 0)
        {
            PublishSignal(newest);
            KeepRangesAhead(newest);
        }
    }

    /// <summary>
    /// Wakes subscribers after a commit or a drained batch of wake-ups.
    /// A lower position still wakes them. The events table is the record.
    /// </summary>
    internal void PublishSignal(long position)
    {
        NoteHead(position);
        if (!PublishNewEventsSignal || position < 0)
            return;

        if (position > 0)
        {
            long observed;
            do
            {
                observed = Volatile.Read(ref _signaledPosition);
                if (position <= observed)
                    break;
            }
            while (Interlocked.CompareExchange(ref _signaledPosition, position, observed) != observed);
        }

        _newEvents.Notify(position);
    }

    /// <summary>
    /// Wakes subscribers after the listen connection is back. The signal carries
    /// no events. Each subscriber reads from its own checkpoint.
    /// </summary>
    internal void WakeSubscribersForCatchUp()
    {
        if (!PublishNewEventsSignal)
            return;

        var position = Volatile.Read(ref _signaledPosition);
        _newEvents.Notify(position > 0 ? position : 0);
    }

    private static async Task RollbackQuietlyAsync(NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        try
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
        }
    }

    private enum AppendReadKind
    {
        None,
        Partial,
        Elsewhere,
        Match
    }

    private readonly record struct CommittedAppendRead(AppendReadKind Kind, AppendResult? Result, Guid ForeignEventId);

    private readonly record struct StoredAppendRow(string StreamId, long Position, long Version, string EventType);

    private async Task<CommittedAppendRead> ReadCommittedAppendAsync(
        string streamId,
        IReadOnlyList<Guid> eventIds,
        IReadOnlyList<string> eventTypes,
        AppendStamp stamp,
        CancellationToken cancellationToken)
    {
        if (FailAppendReadBack)
            throw new NpgsqlException("read-back failed");

        // No attempt took the time yet, so no statement was sent.
        if (eventIds.Count == 0 || !stamp.IsFixed)
            return new CommittedAppendRead(AppendReadKind.None, null, Guid.Empty);

        // Event ids are not unique. Every attempt of this append wrote the same time
        // and got positions above the floor, so the primary key on position and the
        // time index find its rows without reading older ranges. A match also requires
        // this stream and this event name. JSON is stored as jsonb, so the stored text
        // can differ from the bytes that were sent. A hash of those bytes would not
        // match the stored value.
        var tenant = TenantKey.ForStream(streamId, _options.Tenancy, _options.SingleTenantId);
        var sql = $"""
            SELECT e.event_id, e.stream_id, e.global_position, e.version, t.token
            FROM {_names.Events} e
            JOIN {_names.Types} t ON t.id = e.event_type_id
            WHERE e.recorded_at = @recorded_at
              AND e.global_position > @floor
              AND e.tenant_id = @tenant
              AND e.event_id = ANY(@ids)
            """;
        var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = new NpgsqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.Add(new NpgsqlParameter("recorded_at", NpgsqlDbType.TimestampTz) { Value = stamp.RecordedAt });
        command.Parameters.Add(new NpgsqlParameter("floor", NpgsqlDbType.Bigint) { Value = stamp.Floor });
        command.Parameters.Add(new NpgsqlParameter("tenant", tenant));
        command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = eventIds.ToArray() });
        var rows = new Dictionary<Guid, StoredAppendRow>();
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows[reader.GetGuid(0)] = new StoredAppendRow(
                reader.GetString(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetString(4));
        }

        var matches = 0;
        var elsewhere = 0;
        Guid foreign = Guid.Empty;
        var positions = new List<long>(eventIds.Count);
        long version = -1;
        for (var i = 0; i < eventIds.Count; i++)
        {
            if (!rows.TryGetValue(eventIds[i], out var row))
                continue;

            var expectedType = i < eventTypes.Count ? eventTypes[i] : "";
            if (string.Equals(row.StreamId, streamId, StringComparison.Ordinal)
                && string.Equals(row.EventType, expectedType, StringComparison.Ordinal))
            {
                matches++;
                positions.Add(row.Position);
                version = Math.Max(version, row.Version);
            }
            else
            {
                elsewhere++;
                if (foreign == Guid.Empty)
                    foreign = eventIds[i];
            }
        }

        var missing = eventIds.Count - matches - elsewhere;
        if (matches == eventIds.Count)
            return new CommittedAppendRead(AppendReadKind.Match, new AppendResult(positions, null, version), Guid.Empty);
        // One statement writes the whole batch. An id stored at this time on another
        // stream, or under another event name, means none of this call was saved.
        // Unknown is only when some ids are on this stream and the others are absent.
        if (elsewhere > 0)
            return new CommittedAppendRead(AppendReadKind.Elsewhere, null, foreign);
        if (matches > 0 && missing > 0)
            return new CommittedAppendRead(AppendReadKind.Partial, null, Guid.Empty);
        return new CommittedAppendRead(AppendReadKind.None, null, Guid.Empty);
    }

    private static void EnsureStreamLength(string streamId, string paramName)
    {
        if (streamId.Length <= StreamIdMaxLength)
            return;

        throw new ArgumentException(
            $"Stream id is {streamId.Length} characters. The store keeps stream ids to {StreamIdMaxLength} characters so a stream can move between backends. Shorten the id.",
            paramName);
    }

    private async Task EnsureEventTypeIdsAsync(IReadOnlyList<AppendEvent> envelopes, CancellationToken cancellationToken)
    {
        var missing = envelopes.Select(e => e.EventType).Distinct(StringComparer.Ordinal)
            .Where(token => _eventTypes.FindId(token) is null)
            .ToArray();
        if (missing.Length == 0)
            return;

        await _typeCacheRefresh.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            missing = missing.Where(token => _eventTypes.FindId(token) is null).ToArray();
            if (missing.Length == 0)
                return;

            var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var connectionDisposal = connection.ConfigureAwait(false);
            var insert = new NpgsqlCommand(
                $"INSERT INTO {_names.Types} (token) SELECT unnest(@tokens) ON CONFLICT (token) DO NOTHING",
                connection);
            await using var insertDisposal = insert.ConfigureAwait(false);
            insert.Parameters.Add(new NpgsqlParameter("tokens", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = missing });
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await ReloadEventTypeCacheAsync(connection, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _typeCacheRefresh.Release();
        }
    }

    /// <summary>
    /// Writes each catalog event name into the event name table.
    /// A name that is already stored is left out of the insert, so a second
    /// setup does not assign it a new id. Two setups at the same time still
    /// leave one row per name.
    /// </summary>
    private async Task RecordRegisteredEventNamesAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var tokens = CatalogFamilyTokens()
            .Distinct(StringComparer.Ordinal)
            .OrderBy(token => token, StringComparer.Ordinal)
            .ToArray();
        if (tokens.Length == 0)
            return;

        var insert = new NpgsqlCommand(
            $"""
            INSERT INTO {_names.Types} (token)
            SELECT incoming.token
            FROM unnest(@tokens) AS incoming(token)
            WHERE NOT EXISTS (
                SELECT 1 FROM {_names.Types} existing WHERE existing.token = incoming.token
            )
            ON CONFLICT (token) DO NOTHING
            """,
            connection,
            transaction);
        await using var insertDisposal = insert.ConfigureAwait(false);
        insert.Parameters.Add(new NpgsqlParameter("tokens", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = tokens });
        await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private IEnumerable<string> CatalogFamilyTokens()
    {
        if (_session.Catalog is EventTypeCatalog catalog)
        {
            foreach (var family in catalog.EnumerateFamilies())
                yield return family.Token;
        }
    }

    /// <summary>
    /// Reloads the event name map when a requested name is missing.
    /// False means a name is still absent. That name matches no rows.
    /// Names the store already has are still read.
    /// </summary>
    private async Task<bool> EnsureQueryTypesResolvedAsync(Query query, CancellationToken cancellationToken)
    {
        var tokens = QueryTypeTokens(query);
        if (tokens.Count == 0 || tokens.All(token => _eventTypes.FindId(token) is not null))
            return true;

        var reloaded = false;
        await _typeCacheRefresh.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (tokens.All(token => _eventTypes.FindId(token) is not null))
                return true;
            await ReloadEventTypeCacheAsync(cancellationToken).ConfigureAwait(false);
            reloaded = true;
        }
        finally
        {
            _typeCacheRefresh.Release();
        }

        var resolved = tokens.All(token => _eventTypes.FindId(token) is not null);
        if (reloaded && !resolved && AfterEventNameReload is { } afterReload)
            await afterReload(cancellationToken).ConfigureAwait(false);
        return resolved;
    }

    private static List<string> QueryTypeTokens(Query query)
    {
        var tokens = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in query.Items)
        {
            if (item.Types is not { Count: > 0 })
                continue;
            foreach (var token in item.Types)
            {
                if (!string.IsNullOrEmpty(token) && seen.Add(token))
                    tokens.Add(token);
            }
        }

        return tokens;
    }

    private async Task ReloadEventTypeCacheAsync(CancellationToken cancellationToken)
    {
        var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await ReloadEventTypeCacheAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    private async Task ReloadEventTypeCacheAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var command = new NpgsqlCommand($"SELECT id, token FROM {_names.Types}", connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        var rows = new List<(int, string)>();
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            rows.Add((reader.GetInt32(0), reader.GetString(1)));
        _eventTypes.ReplaceAll(rows.Select(row => (row.Item1, row.Item2)));
    }

    private static async Task ExecuteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        var command = new NpgsqlCommand(sql, connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<Stamp?> ReadStampAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        // partition_size came with format 7. Read it through the row so an older
        // stamp table without the column is still read and refused cleanly.
        var sql = $"SELECT s.format, s.partitioning, (to_jsonb(s) ->> 'partition_size')::bigint FROM {_names.SchemaInfo} AS s WHERE s.component = 'events'";
        var command = new NpgsqlCommand(sql, connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var readerDisposal = reader.ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;
        return new Stamp(reader.GetInt32(0), reader.GetBoolean(1), reader.IsDBNull(2) ? null : reader.GetInt64(2));
    }

    private static async Task<bool> RegclassExistsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string qualifiedName,
        CancellationToken cancellationToken)
    {
        var command = new NpgsqlCommand("SELECT to_regclass(@name)::text", connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.Add(new NpgsqlParameter("name", qualifiedName));
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is string { Length: > 0 };
    }

    /// <summary>
    /// The append statement was sent. Callers read the event ids back and do not retry.
    /// </summary>
    private sealed class AppendStatementSentException : Exception
    {
        public AppendStatementSentException(Exception inner)
            : base(inner.Message, inner)
        {
        }
    }

    /// <summary>
    /// The connection closed before commit was sent. The write was not saved.
    /// </summary>
    private sealed class ClosedConnectionBeforeCommitException : Exception
    {
        public ClosedConnectionBeforeCommitException(Exception inner)
            : base(inner.Message, inner)
        {
        }
    }
}
