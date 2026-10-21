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

    private readonly record struct Stamp(int Format, int PartitionCount, bool Partitioning);

    private static NpgsqlDataSource Source(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            IncludeErrorDetail = true
        };
        return Sources.GetOrAdd(builder.ConnectionString, static value => new NpgsqlDataSourceBuilder(value).Build());
    }

    private async Task<AppendResult> AppendWithRetry(
        Func<CancellationToken, Task<AppendResult>> action,
        CancellationToken cancellationToken)
    {
        // The usual append is one autocommit function call: locks, inserts, then
        // Postgres commits that statement only if it finishes. An error response
        // (57P01, 57P02, 57P03, or 08xxx except 08007) means the statement was
        // aborted, so the write is not saved. That case is tried once.
        // A socket break after that call was submitted can mean the implicit
        // commit already happened, so it stays AppendStatementSentException.
        // An explicit transaction (the commit hook) sends BEGIN with the call
        // and COMMIT afterwards. A closure before CommitAsync is the same one
        // try. CommitAsync is never tried again.
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
            catch (ClosedConnectionBeforeCommitException ex) when (!closedConnectionRetried && !cancellationToken.IsCancellationRequested)
            {
                closedConnectionRetried = true;
                RetryClosedConnection(ex.InnerException ?? ex);
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
                RetryClosedConnection(ex);
                attempt--;
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

    private void RetryClosedConnection(Exception exception)
    {
        _dataSource.Clear();
        if (_logger is not null)
            PostgresEventStoreLog.AppendConnectionClosed(_logger, exception);
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
        bool isDcb,
        CancellationToken cancellationToken)
    {
        var tenant = TenantKey.ForStream(streamId);
        var payloads = new string?[envelopes.Count];
        var binaries = new byte[]?[envelopes.Count];
        var metadata = new string?[envelopes.Count];
        var typeIds = new int[envelopes.Count];
        var schemaVersions = new int[envelopes.Count];
        var codecs = new short[envelopes.Count];
        var tagOrds = new List<int>();
        var tagValues = new List<string>();
        var streamTags = new HashSet<string>(StringComparer.Ordinal);

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
            foreach (var tag in envelope.Tags)
            {
                if (string.IsNullOrEmpty(tag))
                    continue;
                tagOrds.Add(i + 1);
                tagValues.Add(tag);
                streamTags.Add(tag);
            }
        }

        BuildLocks(envelopes, condition, out var lockTags, out var lockModes, out var lockStore);
        BuildCondition(condition, out var check, out var all, out var after, out var itemCount, out var typeItems, out var typeCond, out var tagItems, out var tagCond, out var itemsWithTypes, out var itemsWithTags);

        var sql = $"""
            SELECT global_position, version
            FROM {_names.Append}(
                @stream_id, @tenant_id, @expected, @is_dcb, @aggregate_type, @aggregate_id,
                @event_ids, @type_ids, @schema_versions, @codec_ids, @data, @bdata, @metadata,
                @tag_ords, @tag_values, @stream_tags, @partition_hash, @recorded_at, @notify, @notify_channel,
                @lock_tags, @lock_modes, @lock_store, @store_lock_key,
                @check_condition, @condition_all, @condition_after, @item_count,
                @type_items, @type_ids_cond, @tag_items, @tag_values_cond, @items_with_types, @items_with_tags)
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter("stream_id", streamId));
        command.Parameters.Add(new NpgsqlParameter("tenant_id", tenant));
        command.Parameters.Add(new NpgsqlParameter("expected", NpgsqlDbType.Bigint) { Value = (object?)expectedVersion ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("is_dcb", isDcb));
        command.Parameters.Add(new NpgsqlParameter("aggregate_type", StreamIdParser.ExtractAggregateType(streamId)));
        var aggregateId = StreamIdParser.ExtractAggregateId(streamId);
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
        command.Parameters.Add(new NpgsqlParameter("recorded_at", NpgsqlDbType.TimestampTz) { Value = DateTime.UtcNow });
        command.Parameters.Add(new NpgsqlParameter("notify", _options.NotifyOnAppend));
        command.Parameters.Add(new NpgsqlParameter("notify_channel", _names.NotifyChannel));
        command.Parameters.Add(new NpgsqlParameter("lock_tags", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = lockTags });
        command.Parameters.Add(new NpgsqlParameter("lock_modes", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = lockModes });
        command.Parameters.Add(new NpgsqlParameter("lock_store", lockStore));
        command.Parameters.Add(new NpgsqlParameter("store_lock_key", _names.Schema + ":store"));
        command.Parameters.Add(new NpgsqlParameter("check_condition", check));
        command.Parameters.Add(new NpgsqlParameter("condition_all", all));
        command.Parameters.Add(new NpgsqlParameter("condition_after", NpgsqlDbType.Bigint) { Value = (object?)after ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("item_count", itemCount));
        command.Parameters.Add(new NpgsqlParameter("type_items", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = typeItems });
        command.Parameters.Add(new NpgsqlParameter("type_ids_cond", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = typeCond });
        command.Parameters.Add(new NpgsqlParameter("tag_items", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = tagItems });
        command.Parameters.Add(new NpgsqlParameter("tag_values_cond", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = tagCond });
        command.Parameters.Add(new NpgsqlParameter("items_with_types", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = itemsWithTypes });
        command.Parameters.Add(new NpgsqlParameter("items_with_tags", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = itemsWithTags });

        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var positions = new List<long>(envelopes.Count);
                long lastVersion = -1;
                if (AfterAppendStatement is { } afterStatement)
                    await afterStatement(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
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
        catch (PostgresException ex) when (ex.SqlState == "08007")
        {
            // Commit outcome cannot be told. Read the event ids back. Do not retry.
            throw new AppendStatementSentException(ex);
        }
        catch (PostgresException ex) when (PostgresConnectionClosure.HasRetryableClosureSqlState(ex))
        {
            // The server aborted the statement. The implicit commit did not happen.
            throw;
        }
        catch (PostgresException ex)
        {
            throw MapAppendError(ex, expectedVersion, condition?.After);
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
            && transaction is not null
            && (PostgresConnectionClosure.HasTransportBreak(ex) || PostgresConnectionClosure.IsConnectorBroken(connection)))
        {
            // BEGIN is sent with this call. COMMIT is a later round trip, so a
            // broken connection here cannot have committed.
            throw new ClosedConnectionBeforeCommitException(ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The statement was submitted. An autocommit call may already be committed.
            throw new AppendStatementSentException(ex);
        }
    }

    private static Exception MapAppendError(PostgresException exception, long? expectedVersion, long? conditionAfter)
    {
        if (exception.Hint == "event_already_appended" && Guid.TryParse(exception.Detail, out var eventId))
            return new EventAlreadyAppendedException(eventId);

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
            if (!string.IsNullOrEmpty(tag) && !exclusive.Contains(tag))
                shared.Add(tag);
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
            if (item.Tags is not { Count: > 0 } || item.Tags.All(string.IsNullOrEmpty))
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
                if (!string.IsNullOrEmpty(tag))
                    tags.Add(tag);
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
                    if (string.IsNullOrEmpty(tag))
                        continue;
                    tagsI.Add(number);
                    tagsV.Add(tag);
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
            PublishSignal(newest);
    }

    /// <summary>
    /// Wakes subscribers after a commit or a drained batch of wake-ups.
    /// A lower position still wakes them. The events table is the record.
    /// </summary>
    internal void PublishSignal(long position)
    {
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

    private async Task<AppendResult?> TryReadCommittedAppendAsync(
        string streamId,
        IReadOnlyList<Guid> eventIds,
        CancellationToken cancellationToken)
    {
        if (FailAppendReadBack)
            throw new NpgsqlException("read-back failed");

        if (eventIds.Count == 0)
            return null;

        var tenant = TenantKey.ForStream(streamId);
        var sql = $"""
            SELECT event_id, global_position, version
            FROM {_names.Events}
            WHERE tenant_id = @tenant AND event_id = ANY(@ids)
            """;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("tenant", tenant));
        command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = eventIds.ToArray() });
        var rows = new Dictionary<Guid, (long Position, long Version)>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            rows[reader.GetGuid(0)] = (reader.GetInt64(1), reader.GetInt64(2));

        if (rows.Count != eventIds.Count)
            return null;

        var positions = new List<long>(eventIds.Count);
        long version = -1;
        foreach (var id in eventIds)
        {
            if (!rows.TryGetValue(id, out var row))
                return null;
            positions.Add(row.Position);
            version = Math.Max(version, row.Version);
        }

        return new AppendResult(positions, null, version);
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

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var insert = new NpgsqlCommand(
                $"INSERT INTO {_names.Types} (token) SELECT unnest(@tokens) ON CONFLICT (token) DO NOTHING",
                connection);
            insert.Parameters.Add(new NpgsqlParameter("tokens", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = missing });
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await ReloadEventTypeCacheAsync(connection, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _typeCacheRefresh.Release();
        }
    }

    private async Task<bool> EnsureQueryTypesResolvedAsync(Query query, CancellationToken cancellationToken)
    {
        var tokens = QueryTypeTokens(query);
        if (tokens.Count == 0 || tokens.All(token => _eventTypes.FindId(token) is not null))
            return true;

        await _typeCacheRefresh.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (tokens.All(token => _eventTypes.FindId(token) is not null))
                return true;
            await ReloadEventTypeCacheAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _typeCacheRefresh.Release();
        }

        return tokens.All(token => _eventTypes.FindId(token) is not null);
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
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await ReloadEventTypeCacheAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    private async Task ReloadEventTypeCacheAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand($"SELECT id, token FROM {_names.Types}", connection);
        var rows = new List<(int, string)>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
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
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<Stamp?> ReadStampAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var sql = $"SELECT format, partition_count, partitioning FROM {_names.SchemaInfo} WHERE component = 'events'";
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;
        return new Stamp(reader.GetInt32(0), reader.GetInt32(1), reader.GetBoolean(2));
    }

    private static async Task<bool> RegclassExistsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string qualifiedName,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT to_regclass(@name)::text", connection, transaction);
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
