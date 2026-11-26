using System.Text;
using Microsoft.Data.SqlClient;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Builds DCB <see cref="Query"/> SQL. Tag items are driven from the
/// <c>EventTags</c> clustered index (seek + intersection join) by tag id.
/// A tag item filters <c>EventTypeId</c> on that row. The events table is
/// joined to return the matching rows, and for a timestamp or partition filter.
/// <c>Events.Tags</c> is a read projection and is never filtered.
/// </summary>
internal static class DcbQuerySql
{
    internal const string EventColumns =
        "s.StreamId, e.Version, e.SequencePosition, e.EventTypeId, e.EventData, e.SchemaVersion, e.CodecId, e.Tags, e.Metadata, e.Timestamp";

    /// <summary>
    /// One seek per event. A hash of the stream registry would read every stream
    /// before a short page stopped.
    /// </summary>
    private static string StreamJoin(string qRegistry) =>
        $"INNER LOOP JOIN {qRegistry} s ON s.StreamKey = e.StreamKey";

    /// <summary>
    /// Rows whose rowversion is still owned by an open transaction, or that
    /// were written after the oldest open rowversion, stay out of the result.
    /// Callers omit this predicate after <c>ReadHorizonGracePeriod</c> elapses.
    /// </summary>
    internal const string StableHorizon = "e.[RowVersion] < MIN_ACTIVE_ROWVERSION()";

    /// <summary>
    /// Lookup of one event by global position. The seek uses the clustered position key.
    /// </summary>
    private static string EventsLookupHint(bool applyStableHorizon)
        => applyStableHorizon ? " WITH (FORCESEEK)" : " WITH (READPAST, FORCESEEK)";

    /// <summary>
    /// Seek of one tag id. A later tag in the same item is a point seek on
    /// (tag id, global position), so a short page stops after the first matches.
    /// </summary>
    private static string TagSeekHint(bool applyStableHorizon)
        => applyStableHorizon ? " WITH (FORCESEEK)" : " WITH (READPAST, FORCESEEK)";

    internal enum Mode
    {
        Events,
        MaxSequence
    }

    internal readonly record struct Result(string Sql, List<SqlParameter> Parameters, bool ShortCircuited = false)
    {
        public static Result Empty { get; } = new("", [], ShortCircuited: true);
    }

    public static Result Build(
        string qTable,
        string qTags,
        string qRegistry,
        Query query,
        Func<string, int?> resolveTypeId,
        Func<string, int?> resolveTagId,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        int? limit,
        Mode mode,
        bool applyStableHorizon = true)
    {
        ArgumentNullException.ThrowIfNull(resolveTypeId);
        ArgumentNullException.ThrowIfNull(resolveTagId);

        var parameters = new List<SqlParameter>();
        AddWindowParameters(parameters, fromSequencePosition, toSequencePosition, toTimestamp);

        var items = query.Items;
        if (items.Count == 0)
        {
            var allSql = BuildEventsScan(qTable, qRegistry, fromSequencePosition, toSequencePosition, toTimestamp, limit, mode, parameters, applyStableHorizon);
            return new Result(allSql, parameters);
        }

        var sources = new List<ItemSource>(items.Count);
        for (var i = 0; i < items.Count; i++)
        {
            var source = BuildItemSource(
                qTable, qTags, items[i], i, resolveTypeId, resolveTagId,
                fromSequencePosition, toSequencePosition, toTimestamp, parameters, applyStableHorizon);
            if (source is { } built)
                sources.Add(built);
        }

        if (sources.Count == 0)
            return Result.Empty;

        string sql;
        if (sources.Count == 1)
        {
            sql = ComposeSingle(sources[0], qTable, qRegistry, limit, mode, parameters, applyStableHorizon);
        }
        else
        {
            sql = ComposeUnion(sources, qTable, qRegistry, limit, mode, parameters, applyStableHorizon);
        }

        return new Result(sql, parameters);
    }

    private static void AddWindowParameters(
        List<SqlParameter> parameters,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp)
    {
        if (fromSequencePosition.HasValue)
            parameters.Add(new SqlParameter("@FromSequencePosition", fromSequencePosition.Value));
        if (toSequencePosition.HasValue)
            parameters.Add(new SqlParameter("@ToSequencePosition", toSequencePosition.Value));
        if (toTimestamp.HasValue)
            parameters.Add(new SqlParameter("@ToTimestamp", toTimestamp.Value));
    }

    private static string BuildEventsScan(
        string qTable,
        string qRegistry,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        int? limit,
        Mode mode,
        List<SqlParameter> parameters,
        bool applyStableHorizon)
    {
        var where = BuildEventsWindowWhere("e", fromSequencePosition, toSequencePosition, toTimestamp, applyStableHorizon);
        var readPast = applyStableHorizon ? "" : " WITH (READPAST)";
        if (mode == Mode.MaxSequence)
        {
            return $"""
                SELECT ISNULL(MAX(e.SequencePosition), 0)
                FROM {qTable} e{readPast}
                {where}
                """;
        }

        var sql = $"""
            SELECT {EventColumns}
            FROM {qTable} e{readPast}
            {StreamJoin(qRegistry)}
            {where}
            ORDER BY e.SequencePosition
            """;
        return AppendFetch(sql, limit, parameters);
    }

    private readonly record struct ItemSource(
        string FromClause,
        string WhereClause,
        string SequenceColumn,
        bool IncludesEvents);

    private static ItemSource? BuildItemSource(
        string qTable,
        string qTags,
        QueryItem item,
        int itemIndex,
        Func<string, int?> resolveTypeId,
        Func<string, int?> resolveTagId,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        List<SqlParameter> parameters,
        bool applyStableHorizon)
    {
        IReadOnlyList<int>? typeIds = null;
        if (item.Types is { Count: > 0 })
        {
            var ids = new List<int>(item.Types.Count);
            foreach (var token in item.Types)
            {
                var id = resolveTypeId(token);
                if (id.HasValue)
                    ids.Add(id.Value);
            }

            if (ids.Count == 0)
                return null;

            typeIds = ids;
        }

        int[]? tagIds = null;
        if (item.Tags is { Count: > 0 })
        {
            var ids = new int[item.Tags.Count];
            for (var j = 0; j < item.Tags.Count; j++)
            {
                var id = resolveTagId(item.Tags[j].ToString());
                if (id is null)
                    return null;

                ids[j] = id.Value;
            }

            tagIds = ids;
        }

        var hasTags = tagIds is { Length: > 0 };
        // Event type lives on the tag row. The events table is needed for a
        // timestamp, a partition filter, or an item that has no tags.
        var needsEvents = item.PartitionFilter != null
            || toTimestamp.HasValue
            || !hasTags;

        var from = new StringBuilder();
        var where = new StringBuilder("WHERE 1=1");
        var readPast = applyStableHorizon ? "" : " WITH (READPAST)";
        var tagHint = TagSeekHint(applyStableHorizon);
        string seqColumn;

        if (hasTags)
        {
            from.Append(qTags).Append(" et").Append(itemIndex).Append("_0").Append(tagHint);
            for (var j = 1; j < tagIds!.Length; j++)
            {
                var alias = $"et{itemIndex}_{j}";
                var paramName = $"@Tag{itemIndex}_{j}";
                // Point seek on (tag id, position). A hash of every row for the tag
                // would read the whole tag before the page limit stops it.
                from.Append(" INNER LOOP JOIN ").Append(qTags).Append(' ').Append(alias).Append(tagHint)
                    .Append(" ON ").Append(alias).Append(".TagId = ").Append(paramName)
                    .Append(" AND ").Append(alias).Append(".GlobalSequencePosition = et").Append(itemIndex)
                    .Append("_0.GlobalSequencePosition");
                parameters.Add(new SqlParameter(paramName, tagIds[j]));
            }

            var tag0 = $"@Tag{itemIndex}_0";
            where.Append(" AND et").Append(itemIndex).Append("_0.TagId = ").Append(tag0);
            parameters.Add(new SqlParameter(tag0, tagIds[0]));
            AppendSequenceWindow(where, $"et{itemIndex}_0.GlobalSequencePosition", fromSequencePosition, toSequencePosition);
            AppendTypeIn(where, $"et{itemIndex}_0.EventTypeId", typeIds, itemIndex, parameters);
            seqColumn = $"et{itemIndex}_0.GlobalSequencePosition";

            if (needsEvents)
            {
                from.Append(" INNER LOOP JOIN ").Append(qTable).Append(" e").Append(EventsLookupHint(applyStableHorizon))
                    .Append(" ON e.SequencePosition = et")
                    .Append(itemIndex).Append("_0.GlobalSequencePosition");
                if (toTimestamp.HasValue)
                    where.Append(" AND e.Timestamp <= @ToTimestamp");
                AppendPartition(where, item, itemIndex, parameters);
                seqColumn = "e.SequencePosition";
            }
        }
        else
        {
            from.Append(qTable).Append(" e").Append(readPast);
            AppendSequenceWindow(where, "e.SequencePosition", fromSequencePosition, toSequencePosition);
            if (toTimestamp.HasValue)
                where.Append(" AND e.Timestamp <= @ToTimestamp");

            AppendTypeIn(where, "e.EventTypeId", typeIds, itemIndex, parameters);
            AppendPartition(where, item, itemIndex, parameters);
            seqColumn = "e.SequencePosition";
        }

        if (applyStableHorizon && needsEvents)
            where.Append(" AND ").Append(StableHorizon);

        return new ItemSource(from.ToString(), where.ToString(), seqColumn, needsEvents);
    }

    private static string ComposeSingle(
        ItemSource source,
        string qTable,
        string qRegistry,
        int? limit,
        Mode mode,
        List<SqlParameter> parameters,
        bool applyStableHorizon)
    {
        var horizonAnd = applyStableHorizon ? "AND " + StableHorizon : "";
        var lookup = EventsLookupHint(applyStableHorizon);
        if (mode == Mode.MaxSequence)
        {
            if (!source.IncludesEvents)
            {
                return $"""
                    SELECT ISNULL(MAX({source.SequenceColumn}), 0)
                    FROM {source.FromClause}
                    {source.WhereClause}
                      AND EXISTS (
                        SELECT 1
                        FROM {qTable} e{lookup}
                        WHERE e.SequencePosition = {source.SequenceColumn}
                          {horizonAnd}
                      )
                    """;
            }

            return $"""
                SELECT ISNULL(MAX({source.SequenceColumn}), 0)
                FROM {source.FromClause}
                {source.WhereClause}
                """;
        }

        string sql;
        if (source.IncludesEvents)
        {
            sql = $"""
                SELECT {EventColumns}
                FROM {source.FromClause}
                {StreamJoin(qRegistry)}
                {source.WhereClause}
                ORDER BY {source.SequenceColumn}
                """;
        }
        else
        {
            sql = $"""
                SELECT {EventColumns}
                FROM {source.FromClause}
                INNER LOOP JOIN {qTable} e{lookup} ON e.SequencePosition = {source.SequenceColumn}
                {StreamJoin(qRegistry)}
                {source.WhereClause}
                  {horizonAnd}
                ORDER BY {source.SequenceColumn}
                """;
        }

        return AppendFetch(sql, limit, parameters);
    }

    private static string ComposeUnion(
        List<ItemSource> sources,
        string qTable,
        string qRegistry,
        int? limit,
        Mode mode,
        List<SqlParameter> parameters,
        bool applyStableHorizon)
    {
        var horizonAnd = applyStableHorizon ? "AND " + StableHorizon : "";
        var lookup = EventsLookupHint(applyStableHorizon);
        var union = new StringBuilder();
        for (var i = 0; i < sources.Count; i++)
        {
            if (i > 0)
                union.AppendLine().AppendLine("UNION");
            union.Append("SELECT ").Append(sources[i].SequenceColumn).Append(" AS SequencePosition")
                .AppendLine()
                .Append("FROM ").Append(sources[i].FromClause).AppendLine()
                .Append(sources[i].WhereClause);
        }

        if (mode == Mode.MaxSequence)
        {
            return $"""
                SELECT ISNULL(MAX(e.SequencePosition), 0)
                FROM (
                {union}
                ) m
                INNER LOOP JOIN {qTable} e{lookup} ON e.SequencePosition = m.SequencePosition
                WHERE 1 = 1
                  {horizonAnd}
                """;
        }

        var sql = $"""
            SELECT {EventColumns}
            FROM (
            {union}
            ) m
            INNER LOOP JOIN {qTable} e{lookup} ON e.SequencePosition = m.SequencePosition
            {StreamJoin(qRegistry)}
            WHERE 1 = 1
              {horizonAnd}
            ORDER BY e.SequencePosition
            """;
        return AppendFetch(sql, limit, parameters);
    }

    private static void AppendSequenceWindow(
        StringBuilder where,
        string column,
        long? fromSequencePosition,
        long? toSequencePosition)
    {
        if (fromSequencePosition.HasValue)
            where.Append(" AND ").Append(column).Append(" >= @FromSequencePosition");
        if (toSequencePosition.HasValue)
            where.Append(" AND ").Append(column).Append(" <= @ToSequencePosition");
    }

    private static string BuildEventsWindowWhere(
        string alias,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        bool applyStableHorizon)
    {
        var where = new StringBuilder("WHERE 1=1");
        AppendSequenceWindow(where, $"{alias}.SequencePosition", fromSequencePosition, toSequencePosition);
        if (toTimestamp.HasValue)
            where.Append(" AND ").Append(alias).Append(".Timestamp <= @ToTimestamp");
        if (applyStableHorizon)
            where.Append(" AND ").Append(alias).Append(".[RowVersion] < MIN_ACTIVE_ROWVERSION()");
        return where.ToString();
    }

    private static void AppendTypeIn(
        StringBuilder where,
        string column,
        IReadOnlyList<int>? typeIds,
        int itemIndex,
        List<SqlParameter> parameters)
    {
        if (typeIds is not { Count: > 0 })
            return;

        var names = new string[typeIds.Count];
        for (var j = 0; j < typeIds.Count; j++)
        {
            var paramName = $"@Type{itemIndex}_{j}";
            names[j] = paramName;
            parameters.Add(new SqlParameter(paramName, typeIds[j]));
        }

        where.Append(" AND ").Append(column).Append(" IN (").Append(string.Join(", ", names)).Append(')');
    }

    private static void AppendPartition(
        StringBuilder where,
        QueryItem item,
        int itemIndex,
        List<SqlParameter> parameters)
    {
        if (item.PartitionFilter is not { } pf)
            return;

        where.Append(" AND (e.PartitionHash % @TotalInstances").Append(itemIndex)
            .Append(") = @NodeInstance").Append(itemIndex);
        parameters.Add(new SqlParameter($"@TotalInstances{itemIndex}", pf.TotalInstances));
        parameters.Add(new SqlParameter($"@NodeInstance{itemIndex}", pf.NodeInstance));
    }

    private static string AppendFetch(string sql, int? limit, List<SqlParameter> parameters)
    {
        if (!limit.HasValue)
            return sql;

        parameters.Add(new SqlParameter("@Limit", limit.Value));
        return sql + " OFFSET 0 ROWS FETCH NEXT @Limit ROWS ONLY";
    }
}
