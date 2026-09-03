using System.Text;
using Microsoft.Data.SqlClient;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Builds DCB <see cref="Query"/> SQL. When the EventTags table is enabled, tag items
/// are driven from that clustered index (seek + intersection join) rather than a
/// correlated <c>EXISTS</c> from <c>Events</c>. OPENJSON remains the fallback when
/// <c>UseEventTagsTable</c> is false.
/// </summary>
internal static class DcbQuerySql
{
    internal const string EventColumns =
        "e.StreamId, e.Version, e.SequencePosition, e.EventType, e.EventData, e.ContentType, e.Tags, e.Metadata, e.Timestamp";

    internal enum Mode
    {
        Events,
        MaxSequence
    }

    internal readonly record struct Result(string Sql, List<SqlParameter> Parameters);

    public static Result Build(
        string qTable,
        string qTags,
        bool useEventTagsTable,
        Query query,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        int? limit,
        Mode mode)
    {
        var parameters = new List<SqlParameter>();
        AddWindowParameters(parameters, fromSequencePosition, toSequencePosition, toTimestamp);

        var items = query.Items;
        if (items.Count == 0)
        {
            var allSql = BuildEventsScan(qTable, fromSequencePosition, toSequencePosition, toTimestamp, limit, mode, parameters);
            return new Result(allSql, parameters);
        }

        var sources = new List<ItemSource>(items.Count);
        for (var i = 0; i < items.Count; i++)
            sources.Add(BuildItemSource(qTable, qTags, useEventTagsTable, items[i], i, fromSequencePosition, toSequencePosition, toTimestamp, parameters));

        string sql;
        if (sources.Count == 1)
        {
            sql = ComposeSingle(sources[0], qTable, limit, mode, parameters);
        }
        else
        {
            sql = ComposeUnion(sources, qTable, limit, mode, parameters);
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
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        int? limit,
        Mode mode,
        List<SqlParameter> parameters)
    {
        var where = BuildEventsWindowWhere("e", fromSequencePosition, toSequencePosition, toTimestamp);
        if (mode == Mode.MaxSequence)
        {
            return $"""
                SELECT ISNULL(MAX(e.SequencePosition), 0)
                FROM {qTable} e
                {where}
                """;
        }

        var sql = $"""
            SELECT {EventColumns}
            FROM {qTable} e
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

    private static ItemSource BuildItemSource(
        string qTable,
        string qTags,
        bool useEventTagsTable,
        QueryItem item,
        int itemIndex,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        List<SqlParameter> parameters)
    {
        var hasTags = item.Tags is { Count: > 0 };
        var driveFromTags = useEventTagsTable && hasTags;
        var needsEvents = item.Types is { Count: > 0 }
            || item.PartitionFilter != null
            || toTimestamp.HasValue
            || !driveFromTags;

        var from = new StringBuilder();
        var where = new StringBuilder("WHERE 1=1");
        string seqColumn;

        if (driveFromTags)
        {
            var tags = item.Tags!;
            from.Append(qTags).Append(" et").Append(itemIndex).Append("_0");
            for (var j = 1; j < tags.Count; j++)
            {
                var alias = $"et{itemIndex}_{j}";
                var paramName = $"@Tag{itemIndex}_{j}";
                from.Append(" INNER JOIN ").Append(qTags).Append(' ').Append(alias)
                    .Append(" ON ").Append(alias).Append(".GlobalSequencePosition = et").Append(itemIndex)
                    .Append("_0.GlobalSequencePosition AND ").Append(alias).Append(".Tag = ").Append(paramName);
                parameters.Add(new SqlParameter(paramName, tags[j]));
            }

            var tag0 = $"@Tag{itemIndex}_0";
            where.Append(" AND et").Append(itemIndex).Append("_0.Tag = ").Append(tag0);
            parameters.Add(new SqlParameter(tag0, tags[0]));
            AppendSequenceWindow(where, $"et{itemIndex}_0.GlobalSequencePosition", fromSequencePosition, toSequencePosition);
            seqColumn = $"et{itemIndex}_0.GlobalSequencePosition";

            if (needsEvents)
            {
                from.Append(" INNER JOIN ").Append(qTable).Append(" e ON e.SequencePosition = et")
                    .Append(itemIndex).Append("_0.GlobalSequencePosition");
                AppendEventPredicates(where, item, itemIndex, toTimestamp, parameters);
                seqColumn = "e.SequencePosition";
            }
        }
        else
        {
            from.Append(qTable).Append(" e");
            AppendSequenceWindow(where, "e.SequencePosition", fromSequencePosition, toSequencePosition);
            if (toTimestamp.HasValue)
                where.Append(" AND e.Timestamp <= @ToTimestamp");

            if (hasTags)
            {
                for (var j = 0; j < item.Tags!.Count; j++)
                {
                    var paramName = $"@Tag{itemIndex}_{j}";
                    if (useEventTagsTable)
                    {
                        where.Append(" AND EXISTS (SELECT 1 FROM ").Append(qTags)
                            .Append(" et WHERE et.GlobalSequencePosition = e.SequencePosition AND et.Tag = ")
                            .Append(paramName).Append(')');
                    }
                    else
                    {
                        where.Append(" AND EXISTS (SELECT 1 FROM OPENJSON(e.Tags) WHERE value = ")
                            .Append(paramName).Append(')');
                    }
                    parameters.Add(new SqlParameter(paramName, item.Tags[j]));
                }
            }

            AppendTypeAndPartition(where, item, itemIndex, parameters);
            seqColumn = "e.SequencePosition";
        }

        return new ItemSource(from.ToString(), where.ToString(), seqColumn, needsEvents || !driveFromTags);
    }

    private static string ComposeSingle(
        ItemSource source,
        string qTable,
        int? limit,
        Mode mode,
        List<SqlParameter> parameters)
    {
        if (mode == Mode.MaxSequence)
        {
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
                {source.WhereClause}
                ORDER BY {source.SequenceColumn}
                """;
        }
        else
        {
            // Tag-only source (MAX path): join Events for payload columns.
            sql = $"""
                SELECT {EventColumns}
                FROM {source.FromClause}
                INNER JOIN {qTable} e ON e.SequencePosition = {source.SequenceColumn}
                {source.WhereClause}
                ORDER BY {source.SequenceColumn}
                """;
        }

        return AppendFetch(sql, limit, parameters);
    }

    private static string ComposeUnion(
        List<ItemSource> sources,
        string qTable,
        int? limit,
        Mode mode,
        List<SqlParameter> parameters)
    {
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
                SELECT ISNULL(MAX(m.SequencePosition), 0)
                FROM (
                {union}
                ) m
                """;
        }

        var sql = $"""
            SELECT {EventColumns}
            FROM (
            {union}
            ) m
            INNER JOIN {qTable} e ON e.SequencePosition = m.SequencePosition
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
        DateTime? toTimestamp)
    {
        var where = new StringBuilder("WHERE 1=1");
        AppendSequenceWindow(where, $"{alias}.SequencePosition", fromSequencePosition, toSequencePosition);
        if (toTimestamp.HasValue)
            where.Append(" AND ").Append(alias).Append(".Timestamp <= @ToTimestamp");
        return where.ToString();
    }

    private static void AppendEventPredicates(
        StringBuilder where,
        QueryItem item,
        int itemIndex,
        DateTime? toTimestamp,
        List<SqlParameter> parameters)
    {
        if (toTimestamp.HasValue)
            where.Append(" AND e.Timestamp <= @ToTimestamp");
        AppendTypeAndPartition(where, item, itemIndex, parameters);
    }

    private static void AppendTypeAndPartition(
        StringBuilder where,
        QueryItem item,
        int itemIndex,
        List<SqlParameter> parameters)
    {
        if (item.Types is { Count: > 0 })
        {
            var typeConditions = new List<string>(item.Types.Count);
            for (var j = 0; j < item.Types.Count; j++)
            {
                var paramName = $"@Type{itemIndex}_{j}";
                var paramNamePrefix = $"@TypePrefix{itemIndex}_{j}";
                typeConditions.Add($"(e.EventType = {paramName} OR e.EventType LIKE {paramNamePrefix})");
                parameters.Add(new SqlParameter(paramName, item.Types[j]));
                parameters.Add(new SqlParameter(paramNamePrefix, item.Types[j] + ",%"));
            }
            where.Append(" AND (").Append(string.Join(" OR ", typeConditions)).Append(')');
        }

        if (item.PartitionFilter != null)
        {
            var pf = item.PartitionFilter;
            where.Append(" AND (e.PartitionHash % @TotalInstances").Append(itemIndex)
                .Append(") = @NodeInstance").Append(itemIndex);
            parameters.Add(new SqlParameter($"@TotalInstances{itemIndex}", pf.TotalInstances));
            parameters.Add(new SqlParameter($"@NodeInstance{itemIndex}", pf.NodeInstance));
        }
    }

    private static string AppendFetch(string sql, int? limit, List<SqlParameter> parameters)
    {
        if (!limit.HasValue)
            return sql;

        parameters.Add(new SqlParameter("@Limit", limit.Value));
        return sql + " OFFSET 0 ROWS FETCH NEXT @Limit ROWS ONLY";
    }
}
