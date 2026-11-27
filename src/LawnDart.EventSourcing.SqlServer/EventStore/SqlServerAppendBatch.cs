using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Sql;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

public partial class SqlServerEventStore
{
    private static readonly Regex VersionConflictMessage = new(
        @"Expected version (-?\d+) but current version is (-?\d+)",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private readonly record struct AppendBatchOutcome(
        AppendResult Result,
        string? TenantId,
        IReadOnlyList<(string Tag, int Id)> LearnedTags);

    private readonly record struct WrittenTag(string Text, string Expression, bool Learned);

    /// <summary>Raised when a tag id cannot be stored. The append wrote nothing.</summary>
    internal const int AppendTagRegistrationError = 51209;

    /// <summary>Clears the flag that tells the stream-tag trigger to return. Run before a batch abort.</summary>
    private const string ClearStreamTagSkipSql =
        "EXEC sp_set_session_context @key = N'LawnDart.SkipStreamTags', @value = NULL;";

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
        string? conditionTenant,
        bool isDcb,
        CancellationToken cancellationToken)
    {
        var parameters = new List<SqlParameter>();
        var sql = new StringBuilder();
        sql.AppendLine("SET NOCOUNT ON;");
        // The stream-tag trigger reads this and returns. The batch inserts those rows from the ids it already holds.
        sql.AppendLine("EXEC sp_set_session_context @key = N'LawnDart.SkipStreamTags', @value = 1;");
        AppendLockStatements(sql, parameters, AppendLockPlan(envelopes, condition));
        if (condition is not null)
            AppendConditionCheck(sql, parameters, condition, conditionTenant);

        var writtenTags = AppendWrittenTagRegistration(sql, parameters, envelopes);

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
            BEGIN
                {ClearStreamTagSkipSql}
                THROW 51208, N'The stream row could not be locked.', 1;
            END

            IF @HasExpected = 1 AND @current <> @Expected
            BEGIN
                DECLARE @versionMessage nvarchar(200) =
                    CONCAT(N'Expected version ', @Expected, N' but current version is ', @current);
                {ClearStreamTagSkipSql}
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

        AppendStoredEvents(sql, parameters, envelopes, eventIds, writtenTags);
        var learnedTags = new List<WrittenTag>();
        foreach (var written in writtenTags)
        {
            if (written.Learned)
                learnedTags.Add(written);
        }

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

            {BuildStreamTagInsert(writtenTags)}
            DELETE FROM {_qFence} WHERE Id = @fenceId;
            EXEC sp_set_session_context @key = N'LawnDart.SkipStreamTags', @value = NULL;

            SELECT
                @first AS FirstPosition,
                @increment AS Increment,
                @start AS StartVersion,
                @eventTenant AS EventTenant;
            """);

        if (learnedTags.Count > 0)
        {
            sql.AppendLine("SELECT Ord, TagId FROM (VALUES");
            for (var i = 0; i < learnedTags.Count; i++)
            {
                if (i > 0)
                    sql.AppendLine(",");
                sql.Append('(').Append(i.ToString(CultureInfo.InvariantCulture))
                    .Append(", ").Append(learnedTags[i].Expression).Append(')');
            }

            sql.AppendLine(") AS v(Ord, TagId) ORDER BY Ord;");
        }

        var tenantId = AppendTenantRules.ResolveStreamStorageTenant(streamId, _options.Tenancy, _options.SingleTenantId);
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

        var learned = new List<(string Tag, int Id)>(learnedTags.Count);
        if (learnedTags.Count > 0)
        {
            if (!await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidOperationException("The append batch did not return tag ids.");

            var index = 0;
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (index >= learnedTags.Count)
                    break;
                learned.Add((learnedTags[index].Text, reader.GetInt32(reader.GetOrdinal("TagId"))));
                index++;
            }

            if (index != learnedTags.Count)
                throw new InvalidOperationException("The append batch returned the wrong number of tag ids.");
        }

        var result = isDcb
            ? new AppendResult(positions)
            : new AppendResult(positions, null, start + envelopes.Count - 1);
        return new AppendBatchOutcome(result, eventTenant, learned);
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
        IReadOnlyList<Guid> eventIds,
        IReadOnlyList<WrittenTag> writtenTags)
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

        var byTag = new Dictionary<string, string>(writtenTags.Count, StringComparer.Ordinal);
        foreach (var written in writtenTags)
            byTag[written.Text] = written.Expression;

        var tagStart = 0;
        var pending = new List<(int EventIndex, string Expression)>();
        for (var i = 0; i < envelopes.Count; i++)
        {
            foreach (var tag in envelopes[i].Tags)
            {
                if (tag.IsDefault)
                    continue;
                if (!byTag.TryGetValue(tag.ToString(), out var expression))
                    throw new InvalidOperationException("A written tag was not registered.");
                pending.Add((i, expression));
            }
        }

        while (tagStart < pending.Count)
        {
            var count = Math.Min(400, pending.Count - tagStart);
            sql.Append("INSERT INTO ").Append(_qTags).AppendLine(" (TagId, GlobalSequencePosition, EventTypeId) VALUES");
            for (var n = 0; n < count; n++)
            {
                var row = pending[tagStart + n];
                var eventIndex = row.EventIndex.ToString(CultureInfo.InvariantCulture);
                if (n > 0)
                    sql.AppendLine(",");
                sql.Append('(').Append(row.Expression)
                    .Append(", @first + ").Append(eventIndex)
                    .Append(" * @increment, @EvType").Append(eventIndex).Append(')');
            }

            sql.AppendLine(";");
            tagStart += count;
        }
    }

    /// <summary>
    /// Resolves written tag text to ids before the stream row is locked.
    /// A tag that is already cached is a parameter. A new tag is inserted in this batch.
    /// </summary>
    private List<WrittenTag> AppendWrittenTagRegistration(
        StringBuilder sql,
        List<SqlParameter> parameters,
        IReadOnlyList<AppendEvent> envelopes)
    {
        var written = new List<WrittenTag>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var envelope in envelopes)
        {
            foreach (var tag in envelope.Tags)
            {
                if (tag.IsDefault || !seen.Add(tag.ToString()))
                    continue;

                var ordinal = written.Count.ToString(CultureInfo.InvariantCulture);
                if (_tagIds.TryGetId(tag.ToString(), out var id))
                {
                    var name = "@KTag" + ordinal;
                    parameters.Add(new SqlParameter(name, SqlDbType.Int) { Value = id });
                    written.Add(new WrittenTag(tag.ToString(), name, Learned: false));
                    continue;
                }

                var variable = "@NTag" + ordinal;
                var param = "@NText" + ordinal;
                var gate = "@NGate" + ordinal;
                parameters.Add(new SqlParameter(param, SqlDbType.NVarChar, 256) { Value = tag.ToString() });
                parameters.Add(new SqlParameter(gate, SqlDbType.NVarChar, 255)
                {
                    Value = "LawnDart.tagid:" + TagLockName(AppendLockScope, tag.ToString())
                });
                sql.Append("DECLARE ").Append(variable).Append(" int = (SELECT Id FROM ").Append(_qTagRegistry)
                    .Append(" WHERE Tag = ").Append(param).Append(" COLLATE ").Append(TagCollation).AppendLine(");");
                // The insert runs only for a tag that is not stored yet. The exclusive lock
                // keeps two such inserts from colliding, so this batch has no TRY/CATCH.
                sql.Append("IF ").Append(variable).AppendLine(" IS NULL");
                sql.AppendLine("BEGIN");
                sql.Append("    EXEC @lockCode = sp_getapplock @Resource = ").Append(gate)
                    .AppendLine(", @LockMode = N'Exclusive', @LockOwner = N'Transaction';");
                sql.AppendLine("    IF @lockCode < 0 BEGIN SET @lockMessage = CONCAT(N'Append lock not granted, code ', @lockCode); THROW ")
                    .Append(AppendLockNotGrantedError).AppendLine(", @lockMessage, 1; END");
                sql.Append("    SET ").Append(variable).Append(" = (SELECT Id FROM ").Append(_qTagRegistry)
                    .Append(" WHERE Tag = ").Append(param).Append(" COLLATE ").Append(TagCollation).AppendLine(");");
                sql.Append("    IF ").Append(variable).AppendLine(" IS NULL");
                sql.AppendLine("    BEGIN");
                sql.Append("        INSERT INTO ").Append(_qTagRegistry).Append(" (Tag) VALUES (").Append(param).AppendLine(");");
                sql.Append("        SET ").Append(variable).AppendLine(" = CONVERT(int, SCOPE_IDENTITY());");
                sql.AppendLine("    END");
                sql.AppendLine("END");
                sql.Append("IF ").Append(variable).Append(" IS NULL THROW ").Append(AppendTagRegistrationError)
                    .AppendLine(", N'A tag id could not be registered.', 1;");
                written.Add(new WrittenTag(tag.ToString(), variable, Learned: true));
            }
        }

        return written;
    }

    /// <summary>
    /// The condition statement for tests. It is the same text the append batch sends.
    /// </summary>
    internal (string Sql, List<SqlParameter> Parameters) PreviewAppendCondition(AppendCondition condition, string? conditionTenant = null)
    {
        var sql = new StringBuilder();
        var parameters = new List<SqlParameter>();
        AppendConditionCheck(sql, parameters, condition, conditionTenant);
        return (sql.ToString(), parameters);
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

    /// <summary>
    /// Inserts stream tag ids the batch already resolved. The trigger sees
    /// session context <c>LawnDart.SkipStreamTags</c> and does not look the text up again.
    /// </summary>
    private string BuildStreamTagInsert(IReadOnlyList<WrittenTag> writtenTags)
    {
        if (writtenTags.Count == 0)
            return string.Empty;

        var sql = new StringBuilder();
        sql.Append("INSERT INTO ").Append(_qStreamTags).AppendLine(" (TagId, StreamKey)");
        sql.AppendLine("SELECT v.TagId, @streamKey FROM (VALUES");
        for (var i = 0; i < writtenTags.Count; i++)
        {
            if (i > 0)
                sql.AppendLine(",");
            sql.Append('(').Append(writtenTags[i].Expression).Append(')');
        }

        sql.AppendLine(") AS v(TagId)");
        sql.Append("WHERE v.TagId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ").Append(_qStreamTags)
            .AppendLine(" AS t WHERE t.TagId = v.TagId AND t.StreamKey = @streamKey);");
        return sql.ToString();
    }

    private void AppendConditionCheck(StringBuilder sql, List<SqlParameter> parameters, AppendCondition condition, string? conditionTenant)
    {
        if (conditionTenant is not null)
            parameters.Add(new SqlParameter("@CondTenant", SqlDbType.NVarChar, 100) { Value = conditionTenant });
        var items = condition.FailIfEventsMatch.Items;
        var tagVars = new Dictionary<string, string>(StringComparer.Ordinal);
        var nextTag = 0;
        foreach (var item in items)
        {
            if (item.Tags is not { Count: > 0 })
                continue;
            foreach (var tag in item.Tags)
            {
                var text = tag.ToString();
                if (tagVars.ContainsKey(text))
                    continue;
                var variable = "@CId" + nextTag.ToString(CultureInfo.InvariantCulture);
                nextTag++;
                tagVars[text] = variable;
                if (_tagIds.TryGetId(text, out var cached))
                {
                    sql.Append("DECLARE ").Append(variable).Append(" int = ")
                        .Append(cached.ToString(CultureInfo.InvariantCulture)).AppendLine(";");
                    continue;
                }

                var param = "@CTag" + (nextTag - 1).ToString(CultureInfo.InvariantCulture);
                parameters.Add(new SqlParameter(param, SqlDbType.NVarChar, 256) { Value = text });
                sql.Append("DECLARE ").Append(variable).Append(" int = (SELECT Id FROM ").Append(_qTagRegistry)
                    .Append(" WHERE Tag = ").Append(param).Append(" COLLATE ").Append(TagCollation).AppendLine(");");
            }
        }

        if (condition.After is { } after)
            parameters.Add(new SqlParameter("@After", SqlDbType.BigInt) { Value = after });

        var branches = new List<string>();
        for (var i = 0; i < items.Count; i++)
        {
            var branch = BuildConditionBranch(items[i], i, tagVars, parameters, condition.After.HasValue, conditionTenant is not null);
            if (branch is not null)
                branches.Add(branch);
        }

        if (branches.Count == 0)
        {
            sql.Append("IF EXISTS (SELECT TOP 1 1 FROM ").Append(_qTable).Append(" AS e WHERE 1 = 1");
            if (conditionTenant is not null)
                sql.Append(" AND e.TenantId = @CondTenant");
            if (condition.After.HasValue)
                sql.Append(" AND e.SequencePosition > @After");
            sql.AppendLine(")");
        }
        else
        {
            sql.Append("IF ").Append(string.Join(" OR ", branches)).AppendLine();
        }

        sql.Append("BEGIN ").Append(ClearStreamTagSkipSql)
            .Append("DECLARE @conditionMessage nvarchar(200) = N'Append condition failed: matching events exist'; THROW ")
            .Append(AppendConditionConflictError)
            .AppendLine(", @conditionMessage, 1; END");
    }

    private string? BuildConditionBranch(
        QueryItem item,
        int itemIndex,
        Dictionary<string, string> tagVars,
        List<SqlParameter> parameters,
        bool hasAfter,
        bool filterTenant)
    {
        var hasTags = item.Tags is { Count: > 0 };
        var hasTypes = item.Types is { Count: > 0 };
        if (!hasTags && !hasTypes)
            return null;

        if (hasTags)
        {
            var sql = new StringBuilder();
            var tag0 = tagVars[item.Tags![0].ToString()];
            // A missing tag id matches nothing. The seek runs only when the id is known.
            sql.Append('(').Append(tag0).Append(" IS NOT NULL AND EXISTS (SELECT 1 FROM ").Append(_qTags)
                .Append(" AS et0 WITH (FORCESEEK)");
            for (var j = 1; j < item.Tags!.Count; j++)
            {
                var alias = "et" + j.ToString(CultureInfo.InvariantCulture);
                var tagVar = tagVars[item.Tags[j].ToString()];
                sql.Append(" INNER LOOP JOIN ").Append(_qTags).Append(" AS ").Append(alias).Append(" WITH (FORCESEEK)")
                    .Append(" ON ").Append(alias).Append(".TagId = ").Append(tagVar)
                    .Append(" AND ").Append(alias).Append(".GlobalSequencePosition = et0.GlobalSequencePosition");
            }

            if (filterTenant)
            {
                sql.Append(" INNER LOOP JOIN ").Append(_qTable).Append(" AS e WITH (FORCESEEK)")
                    .Append(" ON e.SequencePosition = et0.GlobalSequencePosition");
            }

            sql.Append(" WHERE et0.TagId = ").Append(tagVars[item.Tags[0].ToString()]);
            if (filterTenant)
                sql.Append(" AND e.TenantId = @CondTenant");
            if (hasAfter)
                sql.Append(" AND et0.GlobalSequencePosition > @After");
            if (hasTypes)
                AppendConditionTypeIn(sql, "et0.EventTypeId", item, itemIndex, parameters);
            sql.Append("))");
            return sql.ToString();
        }

        var events = new StringBuilder();
        events.Append("(EXISTS (SELECT 1 FROM ").Append(_qTable).Append(" AS e WHERE 1 = 1");
        if (filterTenant)
            events.Append(" AND e.TenantId = @CondTenant");
        if (hasAfter)
            events.Append(" AND e.SequencePosition > @After");
        AppendConditionTypeIn(events, "e.EventTypeId", item, itemIndex, parameters);
        events.Append("))");
        return events.ToString();
    }

    private void AppendConditionTypeIn(
        StringBuilder sql,
        string column,
        QueryItem item,
        int itemIndex,
        List<SqlParameter> parameters)
    {
        var names = new List<string>();
        for (var j = 0; j < item.Types!.Count; j++)
        {
            var token = item.Types[j];
            if (string.IsNullOrEmpty(token))
                continue;
            var name = "@CType" + itemIndex.ToString(CultureInfo.InvariantCulture) + "_" + j.ToString(CultureInfo.InvariantCulture);
            names.Add(name);
            parameters.Add(new SqlParameter(name, SqlDbType.NVarChar, 500) { Value = token });
        }

        if (names.Count == 0)
            sql.Append(" AND 1 = 0");
        else
            sql.Append(" AND ").Append(column).Append(" IN (SELECT Id FROM ").Append(_qTypes)
                .Append(" WHERE Token IN (").Append(string.Join(", ", names)).Append("))");
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
