using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using LawnDart.EventStore;
using LawnDart.Sql;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

public partial class SqlServerEventStore
{
    private static readonly Regex VersionConflictMessage = new(
        @"Expected version (-?\d+) but current version is (-?\d+)",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private readonly record struct AppendBatchOutcome(AppendResult Result, string? TenantId);

    /// <summary>
    /// One round trip. Locks, the condition or version check, one position range,
    /// and the inserts all run in this command. Commit is a later call.
    /// A rejected check does not reserve positions.
    /// </summary>
    private async Task<AppendBatchOutcome> ExecuteAppendBatchAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        IReadOnlyList<AppendEvent> envelopes,
        IReadOnlyList<Guid> eventIds,
        long? expectedVersion,
        AppendCondition? condition,
        bool isDcb,
        CancellationToken cancellationToken)
    {
        var parameters = new List<SqlParameter>();
        var sql = new StringBuilder();
        sql.AppendLine("SET NOCOUNT ON;");
        AppendLockStatements(sql, parameters, AppendLockPlan(envelopes, condition));
        if (condition is not null)
            AppendConditionCheck(sql, parameters, condition);

        sql.AppendLine(
            $"""
            DECLARE @streamKey bigint;
            DECLARE @current bigint;
            DECLARE @eventTenant nvarchar(100);
            DECLARE @existingTags nvarchar(max);

            -- One read of the stream version. The check and the insert both use @current.
            SELECT
                @streamKey = StreamKey,
                @current = CurrentVersion,
                @eventTenant = TenantId,
                @existingTags = Tags
            FROM {_qRegistry} WITH (UPDLOCK, ROWLOCK)
            WHERE StreamId = @StreamId;

            IF @streamKey IS NULL
            BEGIN
                BEGIN TRY
                    INSERT INTO {_qRegistry}
                        (StreamKey, StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                         CreatedAt, LastEventAt, EventCount, Tags, Status)
                    VALUES
                        (NEXT VALUE FOR {_qStreamKeySequence}, @StreamId, @TenantId, @AggregateType, @AggregateId, -1, 0,
                         @Now, @Now, 0, N'[]', N'Active');
                    SELECT
                        @streamKey = StreamKey,
                        @current = -1,
                        @existingTags = N'[]',
                        @eventTenant = @TenantId
                    FROM {_qRegistry} WITH (UPDLOCK, ROWLOCK)
                    WHERE StreamId = @StreamId;
                END TRY
                BEGIN CATCH
                    IF ERROR_NUMBER() IN (2601, 2627)
                    BEGIN
                        SELECT
                            @streamKey = StreamKey,
                            @current = CurrentVersion,
                            @eventTenant = TenantId,
                            @existingTags = Tags
                        FROM {_qRegistry} WITH (UPDLOCK, ROWLOCK)
                        WHERE StreamId = @StreamId;
                    END
                    ELSE
                        THROW;
                END CATCH
            END

            IF @streamKey IS NULL
                THROW 51208, N'The stream row could not be locked.', 1;

            IF @HasExpected = 1 AND @current <> @Expected
            BEGIN
                DECLARE @versionMessage nvarchar(200) =
                    CONCAT(N'Expected version ', @Expected, N' but current version is ', @current);
                THROW {AppendVersionConflictError}, @versionMessage, 1;
            END

            DECLARE @start bigint = CASE
                WHEN @IsDcb = 1 THEN 0
                WHEN @current < 0 THEN 1
                ELSE @current + 1
            END;

            -- Rowversion before any position is reserved, so a later commit stays behind this append.
            INSERT INTO {_qFence} DEFAULT VALUES;
            DECLARE @fenceId bigint = CONVERT(bigint, SCOPE_IDENTITY());

            DECLARE @rangeFirst sql_variant;
            DECLARE @rangeIncrement sql_variant;
            EXEC sys.sp_sequence_get_range
                @sequence_name = @SequenceName,
                @range_size = @EventCount,
                @range_first_value = @rangeFirst OUTPUT,
                @sequence_increment = @rangeIncrement OUTPUT;
            DECLARE @first bigint = CONVERT(bigint, @rangeFirst);
            DECLARE @increment bigint = CONVERT(bigint, @rangeIncrement);
            """);

        AppendStoredEvents(sql, parameters, envelopes, eventIds);
        if (_options.EnableOutbox && BatchHasOutbound(envelopes))
            AppendOutboxRows(sql, parameters, envelopes, eventIds);

        // A dynamic consistency boundary append stores current version 0.
        // Stream appends store the last event version.
        // Tags are rewritten only when this append adds one. The stream-tag
        // trigger then stays out of the hot path.
        sql.AppendLine(
            $"""
            DECLARE @registryVersion bigint = CASE WHEN @IsDcb = 1 THEN 0 ELSE @start + @EventCount - 1 END;
            DECLARE @lastPosition bigint = @first + (CONVERT(bigint, @EventCount) - 1) * @increment;
            IF EXISTS (
                SELECT 1
                FROM OPENJSON(@NewTags) AS n
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM OPENJSON(CASE WHEN ISJSON(@existingTags) = 1 THEN @existingTags ELSE N'[]' END) AS e
                    WHERE e.[value] COLLATE {TagCollation} = n.[value] COLLATE {TagCollation}
                )
            )
            BEGIN
                DECLARE @merged nvarchar(max);
                ;WITH src AS (
                    SELECT CONVERT(nvarchar(4000), j.[value]) AS tag, CONVERT(int, j.[key]) AS pos
                    FROM OPENJSON(CASE WHEN ISJSON(@existingTags) = 1 THEN @existingTags ELSE N'[]' END) AS j
                    UNION ALL
                    SELECT CONVERT(nvarchar(4000), j.[value]) AS tag, 1000000 + CONVERT(int, j.[key]) AS pos
                    FROM OPENJSON(@NewTags) AS j
                ),
                ranked AS (
                    SELECT tag, pos,
                           ROW_NUMBER() OVER (
                               PARTITION BY tag COLLATE {TagCollation}
                               ORDER BY pos) AS rn
                    FROM src
                    WHERE tag IS NOT NULL AND tag <> N''
                )
                SELECT @merged = N'[' + ISNULL(STUFF((
                    SELECT N',"' + STRING_ESCAPE(tag, 'json') + N'"'
                    FROM ranked
                    WHERE rn = 1
                    ORDER BY pos
                    FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 1, N''), N'') + N']';

                UPDATE {_qRegistry}
                SET CurrentVersion = @registryVersion,
                    LastSequencePosition = @lastPosition,
                    LastEventAt = @Now,
                    EventCount = EventCount + @EventCount,
                    Tags = @merged
                WHERE StreamId = @StreamId;
            END
            ELSE
            BEGIN
                UPDATE {_qRegistry}
                SET CurrentVersion = @registryVersion,
                    LastSequencePosition = @lastPosition,
                    LastEventAt = @Now,
                    EventCount = EventCount + @EventCount
                WHERE StreamId = @StreamId;
            END

            DELETE FROM {_qFence} WHERE Id = @fenceId;

            SELECT
                @first AS FirstPosition,
                @increment AS Increment,
                @start AS StartVersion,
                @eventTenant AS EventTenant;
            """);

        var tenantId = ExtractTenantId(streamId);
        parameters.Add(new SqlParameter("@StreamId", SqlDbType.NVarChar, 255) { Value = streamId });
        parameters.Add(new SqlParameter("@TenantId", SqlDbType.NVarChar, 100) { Value = (object?)tenantId ?? DBNull.Value });
        parameters.Add(new SqlParameter("@AggregateType", SqlDbType.NVarChar, 100) { Value = ExtractAggregateType(streamId) });
        parameters.Add(new SqlParameter("@AggregateId", SqlDbType.UniqueIdentifier)
        {
            Value = ExtractAggregateId(streamId) is { } aggregateId ? aggregateId : DBNull.Value
        });
        parameters.Add(new SqlParameter("@Now", SqlDbType.DateTime2) { Value = DateTime.UtcNow });
        parameters.Add(new SqlParameter("@HasExpected", SqlDbType.Bit) { Value = expectedVersion.HasValue });
        parameters.Add(new SqlParameter("@Expected", SqlDbType.BigInt) { Value = expectedVersion ?? 0L });
        parameters.Add(new SqlParameter("@IsDcb", SqlDbType.Bit) { Value = isDcb });
        parameters.Add(new SqlParameter("@EventCount", SqlDbType.BigInt) { Value = envelopes.Count });
        parameters.Add(new SqlParameter("@SequenceName", SqlDbType.NVarChar, 776) { Value = _qSequence });
        parameters.Add(new SqlParameter("@PartitionHash", SqlDbType.Int)
        {
            Value = PartitionHashUtility.GetDeterministicHashCode(streamId)
        });
        parameters.Add(new SqlParameter("@NewTags", SqlDbType.NVarChar, -1)
        {
            Value = JsonSerializer.Serialize(UnionTags(envelopes), _jsonOptions)
        });

        var command = new SqlCommand(sql.ToString(), connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddRange(parameters.ToArray());
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var readerDisposal = reader.ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidOperationException("The append batch returned no result.");

        var first = reader.GetInt64(reader.GetOrdinal("FirstPosition"));
        var increment = reader.GetInt64(reader.GetOrdinal("Increment"));
        var start = reader.GetInt64(reader.GetOrdinal("StartVersion"));
        var eventTenant = reader.IsDBNull(reader.GetOrdinal("EventTenant"))
            ? null
            : reader.GetString(reader.GetOrdinal("EventTenant"));
        if (increment < 1)
            throw new InvalidOperationException("The event sequence increment must be a positive whole number.");

        var positions = new long[envelopes.Count];
        for (var i = 0; i < positions.Length; i++)
            positions[i] = first + (i * increment);

        var result = isDcb
            ? new AppendResult(positions)
            : new AppendResult(positions, null, start + envelopes.Count - 1);
        return new AppendBatchOutcome(result, eventTenant);
    }

    private static void AppendLockStatements(
        StringBuilder sql,
        List<SqlParameter> parameters,
        IReadOnlyList<AppendLock> plan)
    {
        sql.AppendLine("DECLARE @lockCode int;");
        sql.AppendLine("DECLARE @lockMessage nvarchar(200);");
        for (var i = 0; i < plan.Count; i++)
        {
            var name = "@L" + i.ToString(CultureInfo.InvariantCulture);
            var mode = plan[i].Exclusive ? "Exclusive" : "Shared";
            sql.Append("EXEC @lockCode = sp_getapplock @Resource = ").Append(name)
                .Append(", @LockMode = N'").Append(mode).AppendLine("', @LockOwner = N'Transaction';");
            sql.Append("IF @lockCode < 0 BEGIN SET @lockMessage = CONCAT(N'Append lock not granted, code ', @lockCode); THROW ")
                .Append(AppendLockNotGrantedError).AppendLine(", @lockMessage, 1; END");
            parameters.Add(new SqlParameter(name, SqlDbType.NVarChar, 255) { Value = plan[i].Resource });
        }
    }

    /// <summary>
    /// Plain multi-row inserts. Parsing JSON inside the insert allocates tempdb
    /// objects, and those pages latch under many writers.
    /// </summary>
    private void AppendStoredEvents(
        StringBuilder sql,
        List<SqlParameter> parameters,
        IReadOnlyList<AppendEvent> envelopes,
        IReadOnlyList<Guid> eventIds)
    {
        const int eventsPerInsert = 200;
        for (var start = 0; start < envelopes.Count; start += eventsPerInsert)
        {
            var count = Math.Min(eventsPerInsert, envelopes.Count - start);
            sql.Append("INSERT INTO ").Append(_qTable).AppendLine(
                " (StreamKey, TenantId, Version, SequencePosition, EventTypeId, EventData, SchemaVersion, CodecId, Tags, Metadata, Timestamp, PartitionHash, EventId) VALUES");
            for (var n = 0; n < count; n++)
            {
                var i = start + n;
                var envelope = envelopes[i];
                if (!_eventTypes.TryGetId(envelope.EventType, out var eventTypeId))
                {
                    throw new InvalidOperationException(
                        $"Event type token '{envelope.EventType}' is not in {_qTypes}. " +
                        "Foreign tokens must be registered before the append transaction opens.");
                }

                var index = i.ToString(CultureInfo.InvariantCulture);
                if (n > 0)
                    sql.AppendLine(",");
                sql.Append("(@StreamKey, @eventTenant, @start + ").Append(index)
                    .Append(", @first + ").Append(index).Append(" * @increment, @EvType").Append(index)
                    .Append(", @EvPayload").Append(index)
                    .Append(", @EvSchema").Append(index)
                    .Append(", @EvCodec").Append(index)
                    .Append(", @EvTags").Append(index)
                    .Append(", @EvMeta").Append(index)
                    .Append(", @Now, @PartitionHash, @EvId").Append(index).Append(')');
                parameters.Add(new SqlParameter("@EvType" + index, SqlDbType.Int) { Value = eventTypeId });
                parameters.Add(BinaryParameter("@EvPayload" + index, envelope.Payload));
                parameters.Add(new SqlParameter("@EvSchema" + index, SqlDbType.Int) { Value = envelope.SchemaVersion });
                parameters.Add(new SqlParameter("@EvCodec" + index, SqlDbType.TinyInt) { Value = envelope.CodecId });
                parameters.Add(new SqlParameter("@EvTags" + index, SqlDbType.NVarChar, 4000)
                {
                    Value = JsonSerializer.Serialize(envelope.Tags, _jsonOptions)
                });
                parameters.Add(new SqlParameter("@EvMeta" + index, SqlDbType.NVarChar, -1)
                {
                    Value = envelope.Metadata.IsEmpty
                        ? DBNull.Value
                        : Encoding.UTF8.GetString(envelope.Metadata.Span)
                });
                parameters.Add(new SqlParameter("@EvId" + index, SqlDbType.UniqueIdentifier) { Value = eventIds[i] });
            }

            sql.AppendLine(";");
        }

        var tagOrdinal = 0;
        var tagStart = 0;
        var pending = new List<(int EventIndex, string Tag)>();
        for (var i = 0; i < envelopes.Count; i++)
        {
            foreach (var tag in envelopes[i].Tags)
            {
                if (!string.IsNullOrEmpty(tag))
                    pending.Add((i, tag));
            }
        }

        while (tagStart < pending.Count)
        {
            var count = Math.Min(400, pending.Count - tagStart);
            sql.Append("INSERT INTO ").Append(_qTags).AppendLine(" (GlobalSequencePosition, Tag) VALUES");
            for (var n = 0; n < count; n++)
            {
                var row = pending[tagStart + n];
                var index = tagOrdinal.ToString(CultureInfo.InvariantCulture);
                if (n > 0)
                    sql.AppendLine(",");
                sql.Append("(@first + ").Append(row.EventIndex.ToString(CultureInfo.InvariantCulture))
                    .Append(" * @increment, @EvTag").Append(index).Append(')');
                parameters.Add(new SqlParameter("@EvTag" + index, SqlDbType.NVarChar, 256) { Value = row.Tag });
                tagOrdinal++;
            }

            sql.AppendLine(";");
            tagStart += count;
        }
    }

    private void AppendOutboxRows(
        StringBuilder sql,
        List<SqlParameter> parameters,
        IReadOnlyList<AppendEvent> envelopes,
        IReadOnlyList<Guid> eventIds)
    {
        var rows = new List<int>();
        for (var i = 0; i < envelopes.Count; i++)
        {
            if (envelopes[i].Outbound is not null)
                rows.Add(i);
        }

        if (rows.Count == 0)
            return;

        var outbox = SqlIdentifier.Qualify(_schemaName, _options.OutboxTableName);
        sql.Append("INSERT INTO ").Append(outbox).AppendLine(
            " (Id, EventType, SchemaVersion, CodecId, Payload, Metadata, CreatedAt, Attempts, StreamId, SequencePosition) VALUES");
        for (var n = 0; n < rows.Count; n++)
        {
            var i = rows[n];
            var outbound = envelopes[i].Outbound!;
            var index = i.ToString(CultureInfo.InvariantCulture);
            if (n > 0)
                sql.AppendLine(",");
            sql.Append("(@EvId").Append(index)
                .Append(", @ObType").Append(index)
                .Append(", @ObSchema").Append(index)
                .Append(", @ObCodec").Append(index)
                .Append(", @ObPayload").Append(index)
                .Append(", @ObMeta").Append(index)
                .Append(", @Now, 0, @StreamId, @first + ").Append(index).Append(" * @increment)");
            parameters.Add(new SqlParameter("@ObType" + index, SqlDbType.NVarChar, 500) { Value = outbound.EventType });
            parameters.Add(new SqlParameter("@ObSchema" + index, SqlDbType.Int) { Value = outbound.SchemaVersion });
            parameters.Add(new SqlParameter("@ObCodec" + index, SqlDbType.TinyInt) { Value = outbound.CodecId });
            parameters.Add(BinaryParameter("@ObPayload" + index, outbound.Payload));
            parameters.Add(new SqlParameter("@ObMeta" + index, SqlDbType.NVarChar, -1)
            {
                Value = outbound.Metadata.IsEmpty ? "{}" : Encoding.UTF8.GetString(outbound.Metadata.Span)
            });
        }

        sql.AppendLine(";");
    }

    private static SqlParameter BinaryParameter(string name, ReadOnlyMemory<byte> payload)
    {
        var bytes = payload.IsEmpty ? Array.Empty<byte>() : payload.ToArray();
        return new SqlParameter(name, SqlDbType.VarBinary, -1) { Value = bytes };
    }

    private void AppendConditionCheck(StringBuilder sql, List<SqlParameter> parameters, AppendCondition condition)
    {
        sql.Append("IF EXISTS (SELECT TOP 1 1 FROM ").Append(_qTable).Append(" AS e WHERE 1 = 1");
        if (condition.After is { } after)
        {
            sql.Append(" AND e.SequencePosition > @After");
            parameters.Add(new SqlParameter("@After", SqlDbType.BigInt) { Value = after });
        }

        var predicates = new List<string>();
        var items = condition.FailIfEventsMatch.Items;
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            var parts = new List<string>();
            if (item.Types is { Count: > 0 })
            {
                var names = new List<string>();
                for (var j = 0; j < item.Types.Count; j++)
                {
                    var token = item.Types[j];
                    if (string.IsNullOrEmpty(token))
                        continue;
                    var name = "@CType" + i.ToString(CultureInfo.InvariantCulture) + "_" + j.ToString(CultureInfo.InvariantCulture);
                    names.Add(name);
                    parameters.Add(new SqlParameter(name, SqlDbType.NVarChar, 500) { Value = token });
                }

                parts.Add(names.Count == 0
                    ? "1 = 0"
                    : "e.EventTypeId IN (SELECT Id FROM " + _qTypes + " WHERE Token IN (" + string.Join(", ", names) + "))");
            }

            if (item.Tags is { Count: > 0 })
            {
                for (var j = 0; j < item.Tags.Count; j++)
                {
                    var name = "@CTag" + i.ToString(CultureInfo.InvariantCulture) + "_" + j.ToString(CultureInfo.InvariantCulture);
                    parts.Add(
                        "EXISTS (SELECT 1 FROM " + _qTags + " AS et WHERE et.GlobalSequencePosition = e.SequencePosition AND et.Tag = " + name + ")");
                    parameters.Add(new SqlParameter(name, SqlDbType.NVarChar, 256) { Value = item.Tags[j] ?? string.Empty });
                }
            }

            if (parts.Count > 0)
                predicates.Add("(" + string.Join(" AND ", parts) + ")");
        }

        if (predicates.Count > 0)
            sql.Append(" AND (").Append(string.Join(" OR ", predicates)).Append(')');

        sql.AppendLine(")");
        sql.Append("BEGIN DECLARE @conditionMessage nvarchar(200) = N'Append condition failed: matching events exist'; THROW ")
            .Append(AppendConditionConflictError)
            .AppendLine(", @conditionMessage, 1; END");
    }


    private static bool HasError(SqlException exception, int number)
    {
        if (exception.Number == number)
            return true;
        foreach (SqlError error in exception.Errors)
        {
            if (error.Number == number)
                return true;
        }

        return false;
    }

    private static ConcurrencyException VersionConflict(SqlException exception)
    {
        foreach (SqlError error in exception.Errors)
        {
            var parsed = ParseVersionConflict(error.Message);
            if (parsed is not null)
                return parsed;
        }

        var fromMessage = ParseVersionConflict(exception.Message);
        if (fromMessage is not null)
            return fromMessage;
        return new ConcurrencyException(exception.Message, exception);
    }

    private static ConcurrencyException? ParseVersionConflict(string message)
    {
        var match = VersionConflictMessage.Match(message);
        if (!match.Success)
            return null;

        var expected = long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
        var actual = long.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture);
        return new ConcurrencyException(
            "Expected version " + expected.ToString(CultureInfo.InvariantCulture)
            + " but current version is " + actual.ToString(CultureInfo.InvariantCulture),
            expected,
            actual);
    }
}
