using System.Text;
using Microsoft.Data.SqlClient;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Builds DCB <see cref="Query"/> SQL. Tag items are driven from the
/// <c>EventTags</c> clustered index (seek + intersection join).
/// Type items are <c>EventTypeId IN (...)</c> from the store cache.
/// <c>Events.Tags</c> is a read projection and is never filtered.
/// </summary>
internal static class DcbQuerySql
{
    internal const string EventColumns =
        "e.StreamId, e.Version, e.SequencePosition, e.EventTypeId, e.EventData, e.SchemaVersion, e.CodecId, e.Tags, e.Metadata, e.Timestamp";

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
        Query query,
        Func<string, int?> resolveTypeId,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        int? limit,
        Mode mode)
    {
        ArgumentNullException.ThrowIfNull(resolveTypeId);

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
        {
            var source = BuildItemSource(
                qTable, qTags, items[i], i, resolveTypeId,
                fromSequencePosition, toSequencePosition, toTimestamp, parameters);
            if (source is { } built)
                sources.Add(built);
        }

        if (sources.Count == 0)
            return Result.Empty;

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

    private static ItemSource? BuildItemSource(
        string qTable,
        string qTags,
        QueryItem item,
        int itemIndex,
        Func<string, int?> resolveTypeId,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        List<SqlParameter> parameters)
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

        var hasTags = item.Tags is { Count: > 0 };
        var needsEvents = typeIds is { Count: > 0 }
            || item.PartitionFilter != null
            || toTimestamp.HasValue
            || !hasTags;

        var from = new StringBuilder();
        var where = new StringBuilder("WHERE 1=1");
        string seqColumn;

        if (hasTags)
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
                AppendEventPredicates(where, typeIds, item, itemIndex, toTimestamp, parameters);
                seqColumn = "e.SequencePosition";
            }
        }
        else
        {
            from.Append(qTable).Append(" e");
            AppendSequenceWindow(where, "e.SequencePosition", fromSequencePosition, toSequencePosition);
            if (toTimestamp.HasValue)
                where.Append(" AND e.Timestamp <= @ToTimestamp");

            AppendTypeAndPartition(where, typeIds, item, itemIndex, parameters);
            seqColumn = "e.SequencePosition";
        }

        return new ItemSource(from.ToString(), where.ToString(), seqColumn, needsEvents || !hasTags);
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
        IReadOnlyList<int>? typeIds,
        QueryItem item,
        int itemIndex,
        DateTime? toTimestamp,
        List<SqlParameter> parameters)
    {
        if (toTimestamp.HasValue)
            where.Append(" AND e.Timestamp <= @ToTimestamp");
        AppendTypeAndPartition(where, typeIds, item, itemIndex, parameters);
    }

    private static void AppendTypeAndPartition(
        StringBuilder where,
        IReadOnlyList<int>? typeIds,
        QueryItem item,
        int itemIndex,
        List<SqlParameter> parameters)
    {
        if (typeIds is { Count: > 0 })
        {
            var names = new string[typeIds.Count];
            for (var j = 0; j < typeIds.Count; j++)
            {
                var paramName = $"@Type{itemIndex}_{j}";
                names[j] = paramName;
                parameters.Add(new SqlParameter(paramName, typeIds[j]));
            }

            where.Append(" AND e.EventTypeId IN (").Append(string.Join(", ", names)).Append(')');
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
