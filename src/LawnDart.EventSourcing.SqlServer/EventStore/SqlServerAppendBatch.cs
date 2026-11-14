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
        AppendLockStatements(sql);
        if (condition is not null)
            AppendConditionCheck(sql, parameters, condition);

        sql.AppendLine(
            $"""
            DECLARE @streamKey bigint;
            DECLARE @current bigint;
            DECLARE @eventTenant nvarchar(100);
            DECLARE @existingTags nvarchar(max);
            DECLARE @newKey TABLE (StreamKey bigint NOT NULL);

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
                    OUTPUT INSERTED.StreamKey INTO @newKey
                    VALUES
                        (NEXT VALUE FOR {_qStreamKeySequence}, @StreamId, @TenantId, @AggregateType, @AggregateId, -1, 0,
                         @Now, @Now, 0, N'[]', N'Active');
                    SELECT @streamKey = StreamKey FROM @newKey;
                    SET @current = -1;
                    SET @existingTags = N'[]';
                    SET @eventTenant = @TenantId;
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
            DECLARE @fenceIds TABLE (Id bigint NOT NULL);
            INSERT INTO {_qFence} OUTPUT INSERTED.Id INTO @fenceIds DEFAULT VALUES;
            DECLARE @fenceId bigint = (SELECT TOP (1) Id FROM @fenceIds);

            DECLARE @rangeFirst sql_variant;
            DECLARE @rangeIncrement sql_variant;
            EXEC sys.sp_sequence_get_range
                @sequence_name = @SequenceName,
                @range_size = @EventCount,
                @range_first_value = @rangeFirst OUTPUT,
                @sequence_increment = @rangeIncrement OUTPUT;
            DECLARE @first bigint = CONVERT(bigint, @rangeFirst);
            DECLARE @increment bigint = CONVERT(bigint, @rangeIncrement);

            INSERT INTO {_qTable}
                (StreamKey, TenantId, Version, SequencePosition, EventTypeId, EventData, SchemaVersion, CodecId,
                 Tags, Metadata, Timestamp, PartitionHash, EventId)
            SELECT
                @streamKey,
                @eventTenant,
                @start + CONVERT(bigint, e.ordinal),
                @first + CONVERT(bigint, e.ordinal) * @increment,
                e.eventTypeId,
                ISNULL(CAST(N'' AS xml).value('xs:base64Binary(sql:column("e.payload"))', 'varbinary(max)'), 0x),
                e.schemaVersion,
                CONVERT(tinyint, e.codecId),
                e.tags,
                e.metadata,
                @Now,
                @PartitionHash,
                e.eventId
            FROM OPENJSON(@Events) WITH (
                ordinal int '$.o',
                eventTypeId int '$.t',
                payload nvarchar(max) '$.p',
                schemaVersion int '$.s',
                codecId int '$.c',
                tags nvarchar(max) '$.g',
                metadata nvarchar(max) '$.m',
                eventId uniqueidentifier '$.i'
            ) AS e;

            INSERT INTO {_qTags} (GlobalSequencePosition, Tag)
            SELECT
                @first + CONVERT(bigint, e.ordinal) * @increment,
                CONVERT(nvarchar(256), tag.[value])
            FROM OPENJSON(@Events) WITH (
                ordinal int '$.o',
                tags nvarchar(max) '$.g'
            ) AS e
            CROSS APPLY OPENJSON(CASE WHEN ISJSON(e.tags) = 1 THEN e.tags ELSE N'[]' END) AS tag
            WHERE tag.[value] IS NOT NULL AND tag.[value] <> N''
            -- A merge join checks the parent key by scanning every event row.
            -- Two appends then deadlock on those shared locks. A loop join seeks
            -- only this batch's positions.
            OPTION (LOOP JOIN);
            """);

        if (_options.EnableOutbox && BatchHasOutbound(envelopes))
        {
            var outbox = SqlIdentifier.Qualify(_schemaName, _options.OutboxTableName);
            sql.AppendLine(
                $"""
                INSERT INTO {outbox}
                    (Id, EventType, SchemaVersion, CodecId, Payload, Metadata, CreatedAt,
                     Attempts, StreamId, SequencePosition)
                SELECT
                    o.eventId,
                    o.eventType,
                    o.schemaVersion,
                    CONVERT(tinyint, o.codecId),
                    ISNULL(CAST(N'' AS xml).value('xs:base64Binary(sql:column("o.payload"))', 'varbinary(max)'), 0x),
                    o.metadata,
                    @Now,
                    0,
                    @StreamId,
                    @first + CONVERT(bigint, o.ordinal) * @increment
                FROM OPENJSON(@Outbox) WITH (
                    ordinal int '$.o',
                    eventId uniqueidentifier '$.i',
                    eventType nvarchar(500) '$.t',
                    schemaVersion int '$.s',
                    codecId int '$.c',
                    payload nvarchar(max) '$.p',
                    metadata nvarchar(max) '$.m'
                ) AS o;
                """);
        }

        // A dynamic consistency boundary append stores current version 0.
        // Stream appends store the last event version.
        sql.AppendLine(
            $"""
            DECLARE @registryVersion bigint = CASE WHEN @IsDcb = 1 THEN 0 ELSE @start + @EventCount - 1 END;
            DECLARE @merged nvarchar(max);
            ;WITH src AS (
                SELECT CONVERT(nvarchar(4000), j.[value]) AS tag, CONVERT(int, j.[key]) AS pos
                FROM OPENJSON(CASE WHEN ISJSON(@existingTags) = 1 THEN @existingTags ELSE N'[]' END) AS j
                UNION ALL
                SELECT CONVERT(nvarchar(4000), j.[value]) AS tag, 1000000 + CONVERT(int, j.[key]) AS pos
                FROM OPENJSON(CASE WHEN ISJSON(@NewTags) = 1 THEN @NewTags ELSE N'[]' END) AS j
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
                LastSequencePosition = @first + (CONVERT(bigint, @EventCount) - 1) * @increment,
                LastEventAt = @Now,
                EventCount = EventCount + @EventCount,
                Tags = @merged
            WHERE StreamId = @StreamId;

            -- Leave this fence row until a later append. Deleting it here holds a
            -- lock that blocks the next append's fence insert until commit.
            -- READPAST skips rows an open append still holds.
            DELETE FROM {_qFence} WITH (READPAST) WHERE Id <> @fenceId;

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
        parameters.Add(new SqlParameter("@Locks", SqlDbType.NVarChar, -1)
        {
            Value = LocksJson(AppendLockPlan(envelopes, condition))
        });
        parameters.Add(new SqlParameter("@Events", SqlDbType.NVarChar, -1)
        {
            Value = EventsJson(envelopes, eventIds)
        });
        parameters.Add(new SqlParameter("@NewTags", SqlDbType.NVarChar, -1)
        {
            Value = JsonSerializer.Serialize(UnionTags(envelopes), _jsonOptions)
        });
        if (_options.EnableOutbox && BatchHasOutbound(envelopes))
        {
            parameters.Add(new SqlParameter("@Outbox", SqlDbType.NVarChar, -1)
            {
                Value = OutboxJson(envelopes, eventIds)
            });
        }

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

    private static void AppendLockStatements(StringBuilder sql)
    {
        sql.AppendLine(
            $"""
            DECLARE @lockCount int = (SELECT COUNT(*) FROM OPENJSON(@Locks));
            DECLARE @lockIndex int = 0;
            DECLARE @lockName nvarchar(255);
            DECLARE @lockMode nvarchar(16);
            DECLARE @lockCode int;
            DECLARE @lockMessage nvarchar(200);
            WHILE @lockIndex < @lockCount
            BEGIN
                SELECT
                    @lockName = JSON_VALUE(@Locks, CONCAT('$[', @lockIndex, '].n')),
                    @lockMode = JSON_VALUE(@Locks, CONCAT('$[', @lockIndex, '].m'));
                EXEC @lockCode = sp_getapplock
                    @Resource = @lockName,
                    @LockMode = @lockMode,
                    @LockOwner = N'Transaction';
                IF @lockCode < 0
                BEGIN
                    SET @lockMessage = CONCAT(N'Append lock not granted, code ', @lockCode);
                    THROW {AppendLockNotGrantedError}, @lockMessage, 1;
                END
                SET @lockIndex += 1;
            END
            """);
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

    private string EventsJson(IReadOnlyList<AppendEvent> envelopes, IReadOnlyList<Guid> eventIds)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartArray();
            for (var i = 0; i < envelopes.Count; i++)
            {
                var envelope = envelopes[i];
                if (!_eventTypes.TryGetId(envelope.EventType, out var eventTypeId))
                {
                    throw new InvalidOperationException(
                        $"Event type token '{envelope.EventType}' is not in {_qTypes}. " +
                        "Foreign tokens must be registered before the append transaction opens.");
                }

                writer.WriteStartObject();
                writer.WriteNumber("o", i);
                writer.WriteNumber("t", eventTypeId);
                writer.WriteString("p", Convert.ToBase64String(envelope.Payload.Span));
                writer.WriteNumber("s", envelope.SchemaVersion);
                writer.WriteNumber("c", envelope.CodecId);
                writer.WriteString("g", JsonSerializer.Serialize(envelope.Tags, _jsonOptions));
                if (envelope.Metadata.IsEmpty)
                    writer.WriteNull("m");
                else
                    writer.WriteString("m", Encoding.UTF8.GetString(envelope.Metadata.Span));
                writer.WriteString("i", eventIds[i]);
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private string OutboxJson(IReadOnlyList<AppendEvent> envelopes, IReadOnlyList<Guid> eventIds)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartArray();
            for (var i = 0; i < envelopes.Count; i++)
            {
                var outbound = envelopes[i].Outbound;
                if (outbound is null)
                    continue;

                writer.WriteStartObject();
                writer.WriteNumber("o", i);
                writer.WriteString("i", eventIds[i]);
                writer.WriteString("t", outbound.EventType);
                writer.WriteNumber("s", outbound.SchemaVersion);
                writer.WriteNumber("c", outbound.CodecId);
                writer.WriteString("p", Convert.ToBase64String(outbound.Payload.Span));
                writer.WriteString(
                    "m",
                    outbound.Metadata.IsEmpty ? "{}" : Encoding.UTF8.GetString(outbound.Metadata.Span));
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static string LocksJson(IReadOnlyList<AppendLock> plan)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartArray();
            foreach (var entry in plan)
            {
                writer.WriteStartObject();
                writer.WriteString("n", entry.Resource);
                writer.WriteString("m", entry.Exclusive ? "Exclusive" : "Shared");
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
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
