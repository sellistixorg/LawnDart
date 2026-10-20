using System.Text;
using LawnDart.EventStore;
using Npgsql;
using NpgsqlTypes;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// Builds a read over events and event_tags. Tag items seek the tag index.
/// Type items use the cached event-name ids. The tags array on the event row is not filtered.
/// </summary>
internal static class PostgresQuerySql
{
    internal const string EventColumns = """
        e.stream_id,
        e.version,
        e.global_position,
        e.event_type_id,
        CASE WHEN e.data IS NOT NULL THEN convert_to(e.data::text, 'UTF8') ELSE e.bdata END AS payload,
        e.schema_version,
        e.codec_id,
        e.tags::text[],
        CASE WHEN e.metadata IS NULL THEN NULL ELSE convert_to(e.metadata::text, 'UTF8') END AS metadata_bytes,
        e.recorded_at
        """;

    internal enum Mode
    {
        Events,
        MaxSequence
    }

    internal readonly record struct Result(string Sql, List<NpgsqlParameter> Parameters, bool ShortCircuited = false)
    {
        public static Result Empty { get; } = new("", [], ShortCircuited: true);
    }

    internal static Result Build(
        PostgresNames names,
        Query query,
        Func<string, int?> resolveTypeId,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toTimestamp,
        int? limit,
        Mode mode)
    {
        var parameters = new List<NpgsqlParameter>();
        AddWindow(parameters, fromSequencePosition, toSequencePosition, toTimestamp);

        if (query.Items.Count == 0)
        {
            return new Result(
                Scan(names.Events, fromSequencePosition, toSequencePosition, toTimestamp, limit, mode, parameters),
                parameters);
        }

        var parts = new List<string>();
        for (var i = 0; i < query.Items.Count; i++)
        {
            var part = Item(names, query.Items[i], i, resolveTypeId, fromSequencePosition, toSequencePosition, toTimestamp, parameters);
            if (part is not null)
                parts.Add(part);
        }

        if (parts.Count == 0)
            return Result.Empty;

        string sql;
        if (mode == Mode.MaxSequence)
        {
            sql = $"""
                SELECT COALESCE(MAX(e.global_position), 0)
                FROM (
                {string.Join("\nUNION\n", parts)}
                ) m
                INNER JOIN {names.Events} e ON e.global_position = m.global_position AND e.tenant_id = m.tenant_id
                """;
        }
        else
        {
            sql = $"""
                SELECT {EventColumns}
                FROM (
                {string.Join("\nUNION\n", parts)}
                ) m
                INNER JOIN {names.Events} e ON e.global_position = m.global_position AND e.tenant_id = m.tenant_id
                ORDER BY e.global_position
                """;
            sql = Limit(sql, limit, parameters);
        }

        return new Result(sql, parameters);
    }

    private static void AddWindow(List<NpgsqlParameter> parameters, long? from, long? to, DateTime? timestamp)
    {
        if (from.HasValue)
            parameters.Add(new NpgsqlParameter("from_seq", NpgsqlDbType.Bigint) { Value = from.Value });
        if (to.HasValue)
            parameters.Add(new NpgsqlParameter("to_seq", NpgsqlDbType.Bigint) { Value = to.Value });
        if (timestamp.HasValue)
            parameters.Add(new NpgsqlParameter("to_ts", NpgsqlDbType.TimestampTz) { Value = timestamp.Value });
    }

    private static string Scan(
        string events,
        long? from,
        long? to,
        DateTime? timestamp,
        int? limit,
        Mode mode,
        List<NpgsqlParameter> parameters)
    {
        var where = Window("e.global_position", from, to, timestamp, "e.recorded_at");
        if (mode == Mode.MaxSequence)
            return $"SELECT COALESCE(MAX(e.global_position), 0) FROM {events} e {where}";

        return Limit($"SELECT {EventColumns} FROM {events} e {where} ORDER BY e.global_position", limit, parameters);
    }

    private static string? Item(
        PostgresNames names,
        QueryItem item,
        int index,
        Func<string, int?> resolveTypeId,
        long? from,
        long? to,
        DateTime? timestamp,
        List<NpgsqlParameter> parameters)
    {
        int[]? typeIds = null;
        if (item.Types is { Count: > 0 })
        {
            var ids = new List<int>();
            foreach (var token in item.Types)
            {
                var id = resolveTypeId(token);
                if (id.HasValue)
                    ids.Add(id.Value);
            }

            if (ids.Count == 0)
                return null;
            typeIds = ids.ToArray();
        }

        var hasTags = item.Tags is { Count: > 0 };
        var builder = new StringBuilder();
        builder.Append("SELECT t0.global_position, t0.tenant_id FROM ");

        if (hasTags)
        {
            var tags = item.Tags!;
            builder.Append(names.Tags).Append(" t0");
            var tag0 = "tag_" + index + "_0";
            parameters.Add(new NpgsqlParameter(tag0, NpgsqlDbType.Text) { Value = tags[0] });
            builder.Append(" WHERE t0.tag = @").Append(tag0);
            for (var j = 1; j < tags.Count; j++)
            {
                var alias = "t" + j;
                var name = "tag_" + index + "_" + j;
                parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Text) { Value = tags[j] });
                builder.Append(" AND EXISTS (SELECT 1 FROM ").Append(names.Tags).Append(' ').Append(alias)
                    .Append(" WHERE ").Append(alias).Append(".global_position = t0.global_position AND ")
                    .Append(alias).Append(".tenant_id = t0.tenant_id AND ")
                    .Append(alias).Append(".tag = @").Append(name).Append(')');
            }

            AppendSeq(builder, "t0.global_position", from, to);
            if (typeIds is not null || item.PartitionFilter is not null || timestamp.HasValue)
            {
                builder.Append(" AND EXISTS (SELECT 1 FROM ").Append(names.Events).Append(" e WHERE e.global_position = t0.global_position AND e.tenant_id = t0.tenant_id");
                AppendTypePartitionTime(builder, typeIds, item, index, timestamp, parameters);
                builder.Append(')');
            }
        }
        else
        {
            builder.Clear();
            builder.Append("SELECT e.global_position, e.tenant_id FROM ").Append(names.Events).Append(" e WHERE 1=1");
            AppendSeq(builder, "e.global_position", from, to);
            if (timestamp.HasValue)
                builder.Append(" AND e.recorded_at <= @to_ts");
            AppendTypePartitionTime(builder, typeIds, item, index, timestamp: null, parameters);
        }

        return builder.ToString();
    }

    private static void AppendTypePartitionTime(
        StringBuilder builder,
        int[]? typeIds,
        QueryItem item,
        int index,
        DateTime? timestamp,
        List<NpgsqlParameter> parameters)
    {
        if (timestamp.HasValue)
            builder.Append(" AND e.recorded_at <= @to_ts");

        if (typeIds is { Length: > 0 })
        {
            var name = "types_" + index;
            parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = typeIds });
            builder.Append(" AND e.event_type_id = ANY(@").Append(name).Append(')');
        }

        if (item.PartitionFilter is { } filter)
        {
            var total = "total_" + index;
            var node = "node_" + index;
            parameters.Add(new NpgsqlParameter(total, NpgsqlDbType.Integer) { Value = filter.TotalInstances });
            parameters.Add(new NpgsqlParameter(node, NpgsqlDbType.Integer) { Value = filter.NodeInstance });
            builder.Append(" AND (e.partition_hash % @").Append(total).Append(") = @").Append(node);
        }
    }

    private static void AppendSeq(StringBuilder builder, string column, long? from, long? to)
    {
        if (from.HasValue)
            builder.Append(" AND ").Append(column).Append(" >= @from_seq");
        if (to.HasValue)
            builder.Append(" AND ").Append(column).Append(" <= @to_seq");
    }

    private static string Window(string column, long? from, long? to, DateTime? timestamp, string timeColumn)
    {
        var builder = new StringBuilder("WHERE 1=1");
        AppendSeq(builder, column, from, to);
        if (timestamp.HasValue)
            builder.Append(" AND ").Append(timeColumn).Append(" <= @to_ts");
        return builder.ToString();
    }

    private static string Limit(string sql, int? limit, List<NpgsqlParameter> parameters)
    {
        if (!limit.HasValue)
            return sql;
        parameters.Add(new NpgsqlParameter("lim", NpgsqlDbType.Integer) { Value = limit.Value });
        return sql + " LIMIT @lim";
    }
}
