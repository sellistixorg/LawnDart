using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Text;
using System.Threading.Channels;
using LawnDart.EventStore;
using Npgsql;
using NpgsqlTypes;

namespace LawnDart.EventSourcing.Postgres.EventStore;

public sealed partial class PostgresEventStore
{
    /// <inheritdoc />
    public async Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await _horizon.ResolveAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    async Task<IReadOnlyList<RecordedEvent>> IEventLog.ReadStreamAsync(
        string streamId, long fromVersion, long? toVersion, DateTime? toCommitTimestamp, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await ReadStreamCoreAsync(connection, streamId, fromVersion, toVersion, toCommitTimestamp, cancellationToken).ConfigureAwait(false);
    }

    async IAsyncEnumerable<RecordedEvent> IEventLog.ReadStreamEnumerableAsync(
        string streamId, long fromVersion, long? toVersion, DateTime? toCommitTimestamp, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var rows = await ((IEventLog)this).ReadStreamAsync(streamId, fromVersion, toVersion, toCommitTimestamp, cancellationToken).ConfigureAwait(false);
        foreach (var row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return row;
        }
    }

    async Task<EventLogQueryResult> IEventLog.ReadByQueryAsync(
        Query query, long? fromSequencePosition, int? limit, long? toSequencePosition, DateTime? toCommitTimestamp, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var read = await ReadQueryCoreAsync(query, fromSequencePosition, limit, toSequencePosition, toCommitTimestamp, cancellationToken).ConfigureAwait(false);
        return new EventLogQueryResult(read.Events) { TypesResolved = read.TypesResolved };
    }

    async IAsyncEnumerable<RecordedEvent> IEventLog.ReadByQueryStreamAsync(
        Query query, long? fromSequencePosition, long? toSequencePosition, DateTime? toCommitTimestamp, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var read = await ReadQueryCoreAsync(query, fromSequencePosition, limit: null, toSequencePosition, toCommitTimestamp, cancellationToken).ConfigureAwait(false);
        foreach (var row in read.Events)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return row;
        }
    }

    async Task<long> IEventLog.GetMaxSequencePositionAsync(
        Query query, long? fromSequencePosition, long? toSequencePosition, DateTime? toCommitTimestamp, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (!await EnsureQueryTypesResolvedAsync(query, cancellationToken).ConfigureAwait(false))
            return 0L;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var head = await _horizon.ResolveAsync(connection, cancellationToken).ConfigureAwait(false);
        var to = Cap(toSequencePosition, head);
        var built = PostgresQuerySql.Build(
            _names, query, _eventTypes.FindId, fromSequencePosition, to, toCommitTimestamp, limit: null, PostgresQuerySql.Mode.MaxSequence);
        if (built.ShortCircuited)
            return 0L;

        await using var command = new NpgsqlCommand(built.Sql, connection);
        foreach (var parameter in built.Parameters)
            command.Parameters.Add(parameter);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is long number ? number : result is null or DBNull ? 0L : Convert.ToInt64(result);
    }

    /// <inheritdoc />
    public async Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default)
    {
        var rows = await QueryStreamsAsync(
            "stream_id = @stream",
            command => command.Parameters.Add(new NpgsqlParameter("stream", streamId)),
            one: true,
            cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(string aggregateType, CancellationToken cancellationToken = default)
        => QueryStreamsAsync("status = 'Active' AND aggregate_type = @type", command => command.Parameters.Add(new NpgsqlParameter("type", aggregateType)), one: false, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(string tag, CancellationToken cancellationToken = default)
        => QueryStreamsAsync("status = 'Active' AND @tag = ANY(tags)", command => command.Parameters.Add(new NpgsqlParameter("tag", tag)), one: false, cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        var (where, parameters) = ActivePrefix(prefix);
        var sql = $"SELECT stream_id FROM {_names.Streams} {where} ORDER BY stream_id";
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
            command.Parameters.Add(parameter);
        var ids = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            ids.Add(reader.GetString(0));
        return ids;
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(long afterSequencePosition, int? limit = null, CancellationToken cancellationToken = default)
    {
        var sqlLimit = limit is > 0 ? " LIMIT @lim" : "";
        return QueryStreamsAsync(
            "status = 'Active' AND last_global_position > @after",
            command =>
            {
                command.Parameters.Add(new NpgsqlParameter("after", afterSequencePosition));
                if (limit is > 0)
                    command.Parameters.Add(new NpgsqlParameter("lim", limit.Value));
            },
            one: false,
            cancellationToken,
            order: "ORDER BY last_global_position" + sqlLimit);
    }

    /// <inheritdoc />
    public async Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        var (where, parameters) = ActivePrefix(prefix);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM {_names.Streams} {where}", connection);
        foreach (var parameter in parameters)
            command.Parameters.Add(parameter);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is long number ? number : Convert.ToInt64(result);
    }

    IEventLogSubscriptionHandle IEventLogSubscriptions.Subscribe(
        string subscriberId, long fromSequence, EventSubscriptionFilter? filter, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(subscriberId))
            throw new ArgumentException("Subscriber id is required.", nameof(subscriberId));
        if (fromSequence < 0)
            throw new ArgumentOutOfRangeException(nameof(fromSequence), "fromSequence cannot be negative.");
        if (_options.SubscriptionChannelCapacity < 1)
            throw new InvalidOperationException("SubscriptionChannelCapacity must be at least 1.");
        if (_options.SubscriptionBatchSize < 1)
            throw new InvalidOperationException("SubscriptionBatchSize must be at least 1.");
        if (_options.BackupPollInterval < TimeSpan.Zero)
            throw new InvalidOperationException("BackupPollInterval cannot be negative.");
        if (_options.PollOnlyInterval < TimeSpan.Zero)
            throw new InvalidOperationException("PollOnlyInterval cannot be negative.");

        filter ??= EventSubscriptionFilter.All();
        _listener.AddUser();
        var handle = new PostgresSubscriptionHandle(
            subscriberId,
            filter,
            _options.SubscriptionChannelCapacity,
            fromSequence,
            cancellationToken,
            candidate =>
            {
                _subscriptions.TryRemove(candidate, out _);
                _listener.RemoveUser();
            });
        if (!_subscriptions.TryAdd(handle, 0))
        {
            _listener.RemoveUser();
            throw new InvalidOperationException("Failed to register subscription handle.");
        }

        _ = Task.Run(() => RunSubscriptionAsync(handle), CancellationToken.None);
        return handle;
    }

    private async Task RunSubscriptionAsync(PostgresSubscriptionHandle handle)
    {
        var cancellation = handle.CancellationToken;
        long lastDelivered = handle.FromSequence - 1;
        var batch = _options.SubscriptionBatchSize;
        var query = handle.Filter.Kind == EventSubscriptionScope.Query
            ? handle.Filter.Query ?? Query.All()
            : Query.All();

        try
        {
            await _listener.Ready.WaitAsync(cancellation).ConfigureAwait(false);
            var hadSuccessfulRead = false;
            var lossAttempts = 0;

            while (!cancellation.IsCancellationRequested && !handle.IsDisposed)
            {
                try
                {
                    var ticket = _newEvents.TakeTicket();
                    var delivered = 0;
                    var head = await GetCurrentSequenceAsync(cancellation).ConfigureAwait(false);
                    var page = await ReadQueryCoreAsync(query, lastDelivered + 1, batch, head, toCommitTimestamp: null, cancellation).ConfigureAwait(false);
                    if (lossAttempts > 0)
                    {
                        if (_logger is not null)
                            PostgresEventStoreLog.SubscriptionConnectionRestored(_logger, handle.SubscriberId);
                        lossAttempts = 0;
                    }

                    hadSuccessfulRead = true;
                    if (!page.TypesResolved)
                    {
                        await WaitPollAsync(ticket, CurrentPoll(), cancellation).ConfigureAwait(false);
                        continue;
                    }

                    foreach (var frame in page.Events)
                    {
                        if (cancellation.IsCancellationRequested || handle.IsDisposed)
                            break;
                        if (frame.SequencePosition <= lastDelivered)
                            continue;
                        if (handle.Filter.Matches(frame))
                        {
                            await handle.WriteAsync(frame, cancellation).ConfigureAwait(false);
                            delivered++;
                        }

                        lastDelivered = frame.SequencePosition;
                        if (delivered >= batch)
                            break;
                    }

                    if (page.Events.Count < batch && head > lastDelivered)
                        lastDelivered = head;
                    if (delivered > 0 || page.Events.Count >= batch)
                        continue;
                    await WaitPollAsync(ticket, CurrentPoll(), cancellation).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsRetryableReadFailure(ex, hadSuccessfulRead) && !cancellation.IsCancellationRequested)
                {
                    lossAttempts++;
                    if (lossAttempts == 1 && _logger is not null)
                        PostgresEventStoreLog.SubscriptionConnectionLost(_logger, ex, handle.SubscriberId);
                    if (lossAttempts > MaxConnectionLossAttempts)
                        throw;

                    var delay = PostgresNotificationListener.BackoffDelay(lossAttempts - 1);
                    try
                    {
                        await Task.Delay(delay, cancellation).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
                    {
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
        }
        catch (ChannelClosedException)
        {
        }
        catch (Exception ex)
        {
            if (_logger is not null)
                PostgresEventStoreLog.SubscriptionFailed(_logger, ex, handle.SubscriberId);
            handle.Fault(new EventSubscriptionFailedException(handle.SubscriberId, ex));
        }
        finally
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
    }

    private const int MaxConnectionLossAttempts = 8;

    private TimeSpan CurrentPoll() =>
        _listener.Mode == PostgresListenMode.Listening
            ? _options.BackupPollInterval
            : _options.PollOnlyInterval;

    internal static bool IsRetryableReadFailure(Exception exception, bool hadSuccessfulRead)
    {
        if (IsConfigurationFailure(exception))
            return false;
        if (IsConnectionLoss(exception) || IsRefusedOrTimeout(exception))
            return true;

        // HostUnreachable is also what a bad address looks like before any read has worked.
        // After a read has worked, the same code means the route dropped, so try again.
        return hadSuccessfulRead && IsHostUnreachable(exception);
    }

    private static bool IsConfigurationFailure(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is AuthenticationException)
                return true;
            if (current is PostgresException postgres
                && postgres.SqlState is "28P01" or "28000" or "3D000")
                return true;
            if (current is SocketException socket
                && socket.SocketErrorCode is SocketError.HostNotFound
                    or SocketError.NoData
                    or SocketError.AddressNotAvailable)
                return true;

            // Npgsql has no exception type or SqlState for a full connection pool.
            if (current is NpgsqlException
                && current.Message.Contains("connection pool has been exhausted", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static bool IsHostUnreachable(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is SocketException socket && socket.SocketErrorCode == SocketError.HostUnreachable)
                return true;
        }

        return false;
    }

    private static bool IsConnectionLoss(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is PostgresException postgres
                && postgres.SqlState is "57P01" or "57P02" or "57P03" or "08003" or "08006")
                return true;
            if (current is SocketException socket
                && socket.SocketErrorCode is SocketError.ConnectionReset
                    or SocketError.ConnectionAborted
                    or SocketError.Shutdown
                    or SocketError.NetworkDown
                    or SocketError.NetworkReset)
                return true;
            // A dropped socket sometimes arrives as a plain IOException with no socket error code.
            if (current is IOException && current is not SocketException
                && (current.Message.Contains("forcibly closed", StringComparison.OrdinalIgnoreCase)
                    || current.Message.Contains("broken pipe", StringComparison.OrdinalIgnoreCase)
                    || current.Message.Contains("transport connection", StringComparison.OrdinalIgnoreCase)))
                return true;
        }

        return false;
    }

    private static bool IsRefusedOrTimeout(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is TimeoutException)
                return true;
            // TryAgain is a temporary name lookup failure, so it is tried again from the first read.
            if (current is SocketException socket
                && socket.SocketErrorCode is SocketError.ConnectionRefused
                    or SocketError.TimedOut
                    or SocketError.TryAgain)
                return true;
        }

        return false;
    }

    private async Task WaitPollAsync(NewEventsTicket ticket, TimeSpan poll, CancellationToken cancellationToken)
    {
        try
        {
            await _newEvents.WaitAsync(ticket, poll, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task<IReadOnlyList<RecordedEvent>> ReadStreamCoreAsync(
        NpgsqlConnection connection,
        string streamId,
        long fromVersion,
        long? toVersion,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        var tenant = TenantKey.ForStream(streamId);
        var sql = $"""
            SELECT {PostgresQuerySql.EventColumns}
            FROM {_names.Events} e
            WHERE e.tenant_id = @tenant AND e.stream_id = @stream AND e.version >= @from
            """;
        if (toVersion.HasValue)
            sql += " AND e.version <= @to";
        if (toCommitTimestamp.HasValue)
            sql += " AND e.recorded_at <= @ts";
        sql += " ORDER BY e.version";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("tenant", tenant));
        command.Parameters.Add(new NpgsqlParameter("stream", streamId));
        command.Parameters.Add(new NpgsqlParameter("from", fromVersion));
        if (toVersion.HasValue)
            command.Parameters.Add(new NpgsqlParameter("to", toVersion.Value));
        if (toCommitTimestamp.HasValue)
            command.Parameters.Add(new NpgsqlParameter("ts", NpgsqlDbType.TimestampTz) { Value = toCommitTimestamp.Value });
        var rows = await ReadRowsAsync(command, cancellationToken).ConfigureAwait(false);
        return await MaterializeAsync(rows, cancellationToken).ConfigureAwait(false);
    }

    private readonly record struct QueryRead(IReadOnlyList<RecordedEvent> Events, bool TypesResolved);

    private async Task<QueryRead> ReadQueryCoreAsync(
        Query query,
        long? fromSequencePosition,
        int? limit,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        if (!await EnsureQueryTypesResolvedAsync(query, cancellationToken).ConfigureAwait(false))
            return new QueryRead([], false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var head = await _horizon.ResolveAsync(connection, cancellationToken).ConfigureAwait(false);
        var to = Cap(toSequencePosition, head);
        var built = PostgresQuerySql.Build(
            _names, query, _eventTypes.FindId, fromSequencePosition, to, toCommitTimestamp, limit, PostgresQuerySql.Mode.Events);
        if (built.ShortCircuited)
            return new QueryRead([], true);

        await using var command = new NpgsqlCommand(built.Sql, connection);
        foreach (var parameter in built.Parameters)
            command.Parameters.Add(parameter);
        var rows = await ReadRowsAsync(command, cancellationToken).ConfigureAwait(false);
        return new QueryRead(await MaterializeAsync(rows, cancellationToken).ConfigureAwait(false), true);
    }

    private static long? Cap(long? requested, long head)
        => requested is null ? head : Math.Min(requested.Value, head);

    private async Task<List<EventRow>> ReadRowsAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var rows = new List<EventRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var tags = reader.IsDBNull(7) ? [] : reader.GetFieldValue<string[]>(7);
            var payload = reader.IsDBNull(4) ? [] : reader.GetFieldValue<byte[]>(4);
            var metadata = reader.IsDBNull(8) ? [] : reader.GetFieldValue<byte[]>(8);
            rows.Add(new EventRow(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt32(3),
                payload,
                reader.GetInt32(5),
                (byte)reader.GetInt16(6),
                tags,
                metadata,
                reader.GetFieldValue<DateTime>(9)));
        }

        return rows;
    }

    private async Task<IReadOnlyList<RecordedEvent>> MaterializeAsync(IReadOnlyList<EventRow> rows, CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
            return [];

        var missing = rows.Select(row => row.EventTypeId).Distinct().Any(id => !_eventTypes.TryGetToken(id, out _));
        if (missing)
            await ReloadEventTypeCacheAsync(cancellationToken).ConfigureAwait(false);

        var frames = new List<RecordedEvent>(rows.Count);
        foreach (var row in rows)
        {
            if (!_eventTypes.TryGetToken(row.EventTypeId, out var token))
            {
                throw new InvalidOperationException(
                    $"Event type id {row.EventTypeId} is not in {_names.Types}. Drop and recreate the schema.");
            }

            var timestamp = row.RecordedAt.Kind == DateTimeKind.Unspecified
                ? DateTime.SpecifyKind(row.RecordedAt, DateTimeKind.Utc)
                : row.RecordedAt.ToUniversalTime();
            frames.Add(new RecordedEvent(
                token,
                row.Payload,
                row.StreamId,
                row.Version,
                row.SequencePosition,
                timestamp,
                row.Metadata,
                row.SchemaVersion,
                row.CodecId,
                row.Tags));
        }

        return frames;
    }

    private async Task<IReadOnlyList<StreamMetadata>> QueryStreamsAsync(
        string predicate,
        Action<NpgsqlCommand> bind,
        bool one,
        CancellationToken cancellationToken,
        string? order = null)
    {
        var sql = $"""
            SELECT stream_id, aggregate_type, aggregate_id, current_version, last_global_position,
                   created_at, last_event_at, event_count, tags::text[], status
            FROM {_names.Streams}
            WHERE {predicate}
            {order ?? "ORDER BY stream_id"}
            """;
        if (one)
            sql += " LIMIT 1";
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        bind(command);
        var list = new List<StreamMetadata>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            list.Add(ReadStream(reader));
        return list;
    }

    private static StreamMetadata ReadStream(NpgsqlDataReader reader)
    {
        var streamId = reader.GetString(0);
        var tags = reader.IsDBNull(8) ? [] : reader.GetFieldValue<string[]>(8);
        return new StreamMetadata
        {
            StreamId = streamId,
            TenantId = StreamIdParser.ExtractTenantId(streamId),
            AggregateType = reader.GetString(1),
            AggregateId = reader.IsDBNull(2) ? null : reader.GetGuid(2),
            CurrentVersion = reader.GetInt64(3),
            LastSequencePosition = reader.GetInt64(4),
            CreatedAt = AsUtc(reader.GetFieldValue<DateTime>(5)),
            LastEventAt = AsUtc(reader.GetFieldValue<DateTime>(6)),
            EventCount = reader.GetInt64(7),
            Tags = tags,
            Status = Enum.Parse<StreamStatus>(reader.GetString(9))
        };
    }

    private static DateTime AsUtc(DateTime value)
        => value.Kind == DateTimeKind.Utc ? value : DateTime.SpecifyKind(value, DateTimeKind.Utc);

    /// <inheritdoc />
    public async Task<StreamPage> ListStreamsAsync(
        StreamListFilter? filter = null,
        string? pageToken = null,
        int pageSize = StreamPage.DefaultPageSize,
        CancellationToken cancellationToken = default)
    {
        StreamPageToken.CheckPageSize(pageSize);
        var after = StreamPageToken.ReadAfter(pageToken, filter);
        var prefix = StreamPageToken.Normalize(filter?.Prefix);
        var tag = StreamPageToken.Normalize(filter?.Tag);
        var aggregateType = StreamPageToken.Normalize(filter?.AggregateType);
        var tenant = prefix is null ? null : TenantKey.ForPrefix(prefix);

        var sql = new StringBuilder();
        var parameters = new List<NpgsqlParameter>
        {
            new("take", pageSize + 1)
        };
        sql.Append("""
            SELECT s.stream_id, s.aggregate_type, s.aggregate_id, s.current_version, s.last_global_position,
                   s.created_at, s.last_event_at, s.event_count, s.tags::text[], s.status
            """);

        if (tag is not null)
        {
            sql.Append(" FROM ").Append(_names.StreamTags).Append(" AS t CROSS JOIN LATERAL (SELECT s.stream_id, s.aggregate_type, s.aggregate_id, s.current_version, s.last_global_position, s.created_at, s.last_event_at, s.event_count, s.tags::text[], s.status FROM ").Append(_names.Streams).Append(" AS s WHERE s.tenant_id = t.tenant_id AND s.stream_id = t.stream_id AND s.status = 'Active'");
            if (prefix is not null)
            {
                sql.Append(" AND s.stream_id LIKE @prefix ESCAPE '\\'");
                parameters.Add(new NpgsqlParameter("prefix", LikePrefix(prefix)));
            }

            if (aggregateType is not null)
            {
                sql.Append(" AND s.aggregate_type = @type");
                parameters.Add(new NpgsqlParameter("type", aggregateType));
            }

            sql.Append(" LIMIT 1) AS s WHERE t.tag = @tag");
            parameters.Add(new NpgsqlParameter("tag", tag));
            if (tenant is not null)
            {
                sql.Append(" AND t.tenant_id = @tenant");
                parameters.Add(new NpgsqlParameter("tenant", tenant));
            }

            if (after is not null)
            {
                sql.Append(" AND t.stream_id > @after");
                parameters.Add(new NpgsqlParameter("after", after));
            }

            sql.Append(" ORDER BY t.stream_id LIMIT @take");
        }
        else
        {
            sql.Append($" FROM {_names.Streams} AS s WHERE s.status = 'Active'");
            if (tenant is not null)
            {
                sql.Append(" AND s.tenant_id = @tenant");
                parameters.Add(new NpgsqlParameter("tenant", tenant));
            }

            if (after is not null)
            {
                sql.Append(" AND s.stream_id > @after");
                parameters.Add(new NpgsqlParameter("after", after));
            }

            if (prefix is not null)
            {
                sql.Append(" AND s.stream_id LIKE @prefix ESCAPE '\\'");
                parameters.Add(new NpgsqlParameter("prefix", LikePrefix(prefix)));
            }

            if (aggregateType is not null)
            {
                sql.Append(" AND s.aggregate_type = @type");
                parameters.Add(new NpgsqlParameter("type", aggregateType));
            }

            sql.Append(" ORDER BY s.stream_id LIMIT @take");
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql.ToString(), connection);
        foreach (var parameter in parameters)
            command.Parameters.Add(parameter);

        var rows = new List<StreamMetadata>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            rows.Add(ReadStream(reader));

        return StreamPageToken.ToPage(rows, pageSize, filter);
    }

    private (string Where, List<NpgsqlParameter> Parameters) ActivePrefix(string? prefix)
    {
        var (where, parameters) = Prefix(prefix);
        if (where.Length == 0)
            return ("WHERE status = 'Active'", parameters);
        return (where + " AND status = 'Active'", parameters);
    }

    private (string Where, List<NpgsqlParameter> Parameters) Prefix(string? prefix)
    {
        if (string.IsNullOrEmpty(prefix))
            return ("", []);

        var parameters = new List<NpgsqlParameter>
        {
            new("prefix", LikePrefix(prefix))
        };
        var where = "WHERE stream_id LIKE @prefix ESCAPE '\\'";
        var tenant = TenantKey.ForPrefix(prefix);
        if (tenant is not null)
        {
            where += " AND tenant_id = @tenant";
            parameters.Add(new NpgsqlParameter("tenant", tenant));
        }

        return (where, parameters);
    }

    private static string LikePrefix(string prefix)
    {
        var escaped = prefix
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("%", "\\%", StringComparison.Ordinal)
            .Replace("_", "\\_", StringComparison.Ordinal);
        return escaped + "%";
    }

    private readonly record struct EventRow(
        string StreamId,
        long Version,
        long SequencePosition,
        int EventTypeId,
        byte[] Payload,
        int SchemaVersion,
        byte CodecId,
        string[] Tags,
        byte[] Metadata,
        DateTime RecordedAt);
}
