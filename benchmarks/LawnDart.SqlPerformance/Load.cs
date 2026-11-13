using System.Data;
using System.Globalization;
using System.Text;
using LawnDart.EventStore;
using LawnDart.Snapshots;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Snapshots;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventSourcing.SqlServer.Snapshots;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace LawnDart.SqlPerformance;

internal static class StoreFactory
{
    public static SqlServerEventStoreOptions SqlOptions(string connectionString) => new()
    {
        ConnectionString = connectionString,
        SchemaName = EngineNames.SqlSchema,
        ContextName = "sqlperf",
        RequireTenantId = true,
        ValidateNames = true,
        EnableOutbox = false,
        EnableStreamRegistry = true,
        ReadHorizonGracePeriod = TimeSpan.FromMinutes(5)
    };

    public static PostgresEventStoreOptions PgOptions(string connectionString) => new()
    {
        ConnectionString = connectionString,
        SchemaName = EngineNames.PgSchema,
        ContextName = "sqlperf",
        RequireTenantId = true,
        ValidateNames = true,
        EnableOutbox = false,
        EnableStreamRegistry = true,
        NotifyOnAppend = true,
        Partitioning = true,
        PartitionCount = 8,
        ReadHorizonGracePeriod = TimeSpan.FromMinutes(5)
    };

    public static async Task<SqlServerEventStore> OpenSqlAsync(string connectionString, CancellationToken cancellationToken)
    {
        var store = new SqlServerEventStore(connectionString, options: SqlOptions(connectionString), enableRegistry: true);
        await store.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        return store;
    }

    public static async Task<PostgresEventStore> OpenPgAsync(string connectionString, CancellationToken cancellationToken)
    {
        var store = new PostgresEventStore(connectionString, options: PgOptions(connectionString));
        await store.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        return store;
    }
}

internal static class Loader
{
    public static async Task LoadAsync(EngineEndpoint endpoint, DatasetPlan plan, CancellationToken cancellationToken)
    {
        if (endpoint.Kind == DatabaseKind.Postgres)
            await LoadPostgresAsync(endpoint, plan, cancellationToken).ConfigureAwait(false);
        else
            await LoadSqlServerAsync(endpoint, plan, cancellationToken).ConfigureAwait(false);
    }

    private static async Task LoadPostgresAsync(EngineEndpoint endpoint, DatasetPlan plan, CancellationToken cancellationToken)
    {
        await StoreFactory.OpenPgAsync(endpoint.DatabaseConnectionString, cancellationToken).ConfigureAwait(false);
        await using var connection = new NpgsqlConnection(endpoint.DatabaseConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ExecAsync(connection, "SET synchronous_commit TO off", cancellationToken).ConfigureAwait(false);
        var types = await InsertPgTypesAsync(connection, cancellationToken).ConfigureAwait(false);
        await ExecAsync(connection, "ALTER TABLE lawndart.streams DISABLE TRIGGER USER", cancellationToken).ConfigureAwait(false);
        var generator = new EventGenerator(plan);
        foreach (var batch in generator.Batches(4000))
            await CopyPostgresAsync(connection, batch, types, cancellationToken).ConfigureAwait(false);
        await ExecAsync(connection, "ALTER TABLE lawndart.streams ENABLE TRIGGER USER", cancellationToken).ConfigureAwait(false);
        await ExecAsync(connection, "SELECT setval('lawndart.event_position', " + plan.Size.ToString(CultureInfo.InvariantCulture) + ", true)", cancellationToken).ConfigureAwait(false);
        await ExecAsync(connection, """
            ANALYZE lawndart.events;
            ANALYZE lawndart.event_tags;
            ANALYZE lawndart.streams;
            ANALYZE lawndart.stream_tags;
            ANALYZE lawndart.event_snapshots;
            """, cancellationToken).ConfigureAwait(false);
        var count = await ScalarAsync(connection, "SELECT COUNT(*) FROM lawndart.events", cancellationToken).ConfigureAwait(false);
        if (count != plan.Size)
            throw new InvalidOperationException("The loaded event count does not match the requested size.");

        var store = await StoreFactory.OpenPgAsync(endpoint.DatabaseConnectionString, cancellationToken).ConfigureAwait(false);
        var snapshots = new PostgresSnapshotStore(StoreFactory.PgOptions(endpoint.DatabaseConnectionString));
        await ShapeProof.AssertAsync(store, snapshots, generator.Catalog(), cancellationToken).ConfigureAwait(false);
    }

    private static async Task LoadSqlServerAsync(EngineEndpoint endpoint, DatasetPlan plan, CancellationToken cancellationToken)
    {
        await StoreFactory.OpenSqlAsync(endpoint.DatabaseConnectionString, cancellationToken).ConfigureAwait(false);
        await using var connection = new SqlConnection(endpoint.DatabaseConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var types = await InsertSqlTypesAsync(connection, cancellationToken).ConfigureAwait(false);
        var generator = new EventGenerator(plan);
        foreach (var batch in generator.Batches(4000))
            await CopySqlAsync(connection, batch, types, cancellationToken).ConfigureAwait(false);

        var next = (plan.Size + 1).ToString(CultureInfo.InvariantCulture);
        await ExecSqlAsync(connection, "ALTER SEQUENCE dbo.EventSequencePosition RESTART WITH " + next, cancellationToken).ConfigureAwait(false);
        await ExecSqlAsync(connection, """
            UPDATE STATISTICS dbo.Events;
            UPDATE STATISTICS dbo.EventTags;
            UPDATE STATISTICS dbo.Streams;
            UPDATE STATISTICS dbo.StreamTags;
            UPDATE STATISTICS dbo.EventSnapshots;
            """, cancellationToken).ConfigureAwait(false);
        var count = await ScalarSqlAsync(connection, "SELECT COUNT_BIG(*) FROM dbo.Events", cancellationToken).ConfigureAwait(false);
        if (count != plan.Size)
            throw new InvalidOperationException("The loaded event count does not match the requested size.");

        var store = await StoreFactory.OpenSqlAsync(endpoint.DatabaseConnectionString, cancellationToken).ConfigureAwait(false);
        var snapshots = new SqlServerSnapshotStore(StoreFactory.SqlOptions(endpoint.DatabaseConnectionString));
        await ShapeProof.AssertAsync(store, snapshots, generator.Catalog(), cancellationToken).ConfigureAwait(false);
    }

    private static async Task CopyPostgresAsync(
        NpgsqlConnection connection,
        LoadBatch batch,
        Dictionary<string, int> types,
        CancellationToken cancellationToken)
    {
        await CopyTextAsync(connection, """
            COPY lawndart.events (
                tenant_id, stream_id, version, global_position, event_id, event_type_id,
                schema_version, codec_id, data, bdata, metadata, tags, partition_hash, recorded_at)
            FROM STDIN
            """, batch.Events, static (row, types, line) =>
        {
            line.Append(TextEscape.Copy(row.Tenant)).Append('\t');
            line.Append(TextEscape.Copy(row.StreamId)).Append('\t');
            line.Append(row.Version.ToString(CultureInfo.InvariantCulture)).Append('\t');
            line.Append(row.Position.ToString(CultureInfo.InvariantCulture)).Append('\t');
            line.Append(row.EventId.ToString("D")).Append('\t');
            line.Append(types[row.EventType].ToString(CultureInfo.InvariantCulture)).Append('\t');
            line.Append(row.SchemaVersion.ToString(CultureInfo.InvariantCulture)).Append('\t');
            line.Append(row.CodecId.ToString(CultureInfo.InvariantCulture)).Append('\t');
            line.Append(TextEscape.Copy(Encoding.UTF8.GetString(row.Payload))).Append('\t');
            line.Append("\\N\t");
            line.Append(TextEscape.Copy(row.MetadataJson)).Append('\t');
            AppendPgArray(line, row.Tags);
            line.Append('\t');
            line.Append(row.PartitionHash.ToString(CultureInfo.InvariantCulture)).Append('\t');
            line.Append(row.Timestamp.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)).Append("+00");
        }, types, cancellationToken).ConfigureAwait(false);

        var tagged = new List<EventRow>();
        foreach (var row in batch.Events)
        {
            if (row.Tags.Length > 0)
                tagged.Add(row);
        }

        if (tagged.Count > 0)
        {
            await CopyTextAsync(connection, "COPY lawndart.event_tags (tag, global_position, tenant_id) FROM STDIN", tagged, static (row, _, line) =>
            {
                for (var i = 0; i < row.Tags.Length; i++)
                {
                    if (i > 0)
                        line.Append('\n');
                    line.Append(TextEscape.Copy(row.Tags[i])).Append('\t');
                    line.Append(row.Position.ToString(CultureInfo.InvariantCulture)).Append('\t');
                    line.Append(TextEscape.Copy(row.Tenant));
                }
            }, types, cancellationToken).ConfigureAwait(false);
        }

        var closed = new List<EventRow>();
        foreach (var row in batch.Events)
        {
            if (row.ClosesStream)
                closed.Add(row);
        }

        if (closed.Count > 0)
        {
            await CopyTextAsync(connection, """
                COPY lawndart.streams (
                    tenant_id, stream_id, aggregate_type, aggregate_id, current_version, last_global_position,
                    created_at, last_event_at, event_count, tags, status)
                FROM STDIN
                """, closed, static (row, _, line) =>
            {
                line.Append(TextEscape.Copy(row.Tenant)).Append('\t');
                line.Append(TextEscape.Copy(row.StreamId)).Append('\t');
                line.Append(TextEscape.Copy(row.AggregateType)).Append('\t');
                line.Append(row.AggregateId.ToString("D")).Append('\t');
                line.Append(row.StreamVersion.ToString(CultureInfo.InvariantCulture)).Append('\t');
                line.Append(row.Position.ToString(CultureInfo.InvariantCulture)).Append('\t');
                line.Append(row.StreamOpenedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)).Append("+00\t");
                line.Append(row.Timestamp.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)).Append("+00\t");
                line.Append(row.StreamEventCount.ToString(CultureInfo.InvariantCulture)).Append('\t');
                AppendPgArray(line, row.StreamTags);
                line.Append("\tActive");
            }, types, cancellationToken).ConfigureAwait(false);

            await CopyTextAsync(connection, "COPY lawndart.stream_tags (tenant_id, tag, stream_id) FROM STDIN", closed, static (row, _, line) =>
            {
                for (var i = 0; i < row.StreamTags.Length; i++)
                {
                    if (i > 0)
                        line.Append('\n');
                    line.Append(TextEscape.Copy(row.Tenant)).Append('\t');
                    line.Append(TextEscape.Copy(row.StreamTags[i])).Append('\t');
                    line.Append(TextEscape.Copy(row.StreamId));
                }
            }, types, cancellationToken).ConfigureAwait(false);
        }

        var snaps = new List<EventRow>();
        foreach (var row in batch.Events)
        {
            if (row.WritesSnapshot)
                snaps.Add(row);
        }

        if (snaps.Count > 0)
        {
            await CopyTextAsync(connection, """
                COPY lawndart.event_snapshots (
                    tenant_id, stream_id, version, global_position, taken_at, state_type, state_data, checksum)
                FROM STDIN
                """, snaps, static (row, _, line) =>
            {
                var state = SnapshotText.For(row.SnapshotVersion);
                line.Append(TextEscape.Copy(row.Tenant)).Append('\t');
                line.Append(TextEscape.Copy(row.StreamId)).Append('\t');
                line.Append(row.SnapshotVersion.ToString(CultureInfo.InvariantCulture)).Append('\t');
                line.Append(row.Position.ToString(CultureInfo.InvariantCulture)).Append('\t');
                line.Append(row.Timestamp.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)).Append("+00\t");
                line.Append(TextEscape.Copy(typeof(BenchState).FullName ?? "BenchState")).Append('\t');
                line.Append("\\\\x").Append(Convert.ToHexString(state.Bytes).ToLowerInvariant()).Append('\t');
                line.Append(state.Checksum);
            }, types, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task CopySqlAsync(
        SqlConnection connection,
        LoadBatch batch,
        Dictionary<string, int> types,
        CancellationToken cancellationToken)
    {
        var events = new List<object?[]>(batch.Events.Count);
        var tags = new List<object?[]>();
        var streams = new List<object?[]>();
        var streamTags = new List<object?[]>();
        var snaps = new List<object?[]>();
        foreach (var row in batch.Events)
        {
            events.Add(
            [
                row.StreamId,
                row.Version,
                row.Position,
                types[row.EventType],
                row.Payload,
                row.SchemaVersion,
                row.CodecId,
                row.TagsJson,
                row.MetadataJson,
                row.Timestamp,
                row.PartitionHash,
                row.EventId
            ]);
            foreach (var tag in row.Tags)
                tags.Add([row.Position, tag]);
            if (row.WritesSnapshot)
            {
                var state = SnapshotText.For(row.SnapshotVersion);
                snaps.Add(
                [
                    row.StreamId,
                    row.SnapshotVersion,
                    row.Position,
                    row.Timestamp,
                    typeof(BenchState).FullName ?? "BenchState",
                    state.Json,
                    state.Checksum
                ]);
            }

            if (!row.ClosesStream)
                continue;
            streams.Add(
            [
                row.StreamId,
                row.Tenant,
                row.AggregateType,
                row.AggregateId,
                row.StreamVersion,
                row.Position,
                row.StreamOpenedAt,
                row.Timestamp,
                row.StreamEventCount,
                row.StreamTagsJson,
                "Active"
            ]);
            foreach (var tag in row.StreamTags)
                streamTags.Add([tag, row.StreamId]);
        }

        await BulkAsync(connection, "dbo.Events",
            ["StreamId", "Version", "SequencePosition", "EventTypeId", "EventData", "SchemaVersion", "CodecId", "Tags", "Metadata", "Timestamp", "PartitionHash", "EventId"],
            [typeof(string), typeof(long), typeof(long), typeof(int), typeof(byte[]), typeof(int), typeof(byte), typeof(string), typeof(string), typeof(DateTime), typeof(int), typeof(Guid)],
            events, cancellationToken).ConfigureAwait(false);
        await BulkAsync(connection, "dbo.EventTags",
            ["GlobalSequencePosition", "Tag"],
            [typeof(long), typeof(string)],
            tags, cancellationToken).ConfigureAwait(false);
        await BulkAsync(connection, "dbo.Streams",
            ["StreamId", "TenantId", "AggregateType", "AggregateId", "CurrentVersion", "LastSequencePosition", "CreatedAt", "LastEventAt", "EventCount", "Tags", "Status"],
            [typeof(string), typeof(string), typeof(string), typeof(Guid), typeof(long), typeof(long), typeof(DateTime), typeof(DateTime), typeof(long), typeof(string), typeof(string)],
            streams, cancellationToken).ConfigureAwait(false);
        await BulkAsync(connection, "dbo.StreamTags",
            ["Tag", "StreamId"],
            [typeof(string), typeof(string)],
            streamTags, cancellationToken).ConfigureAwait(false);
        await BulkAsync(connection, "dbo.EventSnapshots",
            ["StreamId", "Version", "GlobalSequence", "TakenAtUtc", "StateType", "StateData", "Checksum"],
            [typeof(string), typeof(long), typeof(long), typeof(DateTime), typeof(string), typeof(string), typeof(string)],
            snaps, cancellationToken).ConfigureAwait(false);
    }

    private static void AppendPgArray(StringBuilder line, string[] values)
    {
        line.Append('{');
        for (var i = 0; i < values.Length; i++)
        {
            if (i > 0)
                line.Append(',');
            line.Append(TextEscape.Copy(values[i]));
        }

        line.Append('}');
    }

    private static async Task CopyTextAsync(
        NpgsqlConnection connection,
        string command,
        List<EventRow> rows,
        Action<EventRow, Dictionary<string, int>, StringBuilder> write,
        Dictionary<string, int> types,
        CancellationToken cancellationToken)
    {
        var text = new StringBuilder();
        for (var i = 0; i < rows.Count; i++)
        {
            if (text.Length > 0)
                text.Append('\n');
            write(rows[i], types, text);
        }

        if (text.Length == 0)
            return;
        text.Append('\n');
        await using var writer = await connection.BeginTextImportAsync(command, cancellationToken).ConfigureAwait(false);
        await writer.WriteAsync(text, cancellationToken).ConfigureAwait(false);
    }

    private static async Task BulkAsync(
        SqlConnection connection,
        string table,
        string[] names,
        Type[] types,
        List<object?[]> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
            return;

        using var reader = new ObjectReader(names, types, rows);
        using var bulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.TableLock, null)
        {
            DestinationTableName = table,
            BatchSize = rows.Count,
            BulkCopyTimeout = 0
        };
        foreach (var name in names)
            bulk.ColumnMappings.Add(name, name);
        await bulk.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<Dictionary<string, int>> InsertPgTypesAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        foreach (var name in EventGenerator.EventTypeNames)
        {
            await using var command = new NpgsqlCommand(
                "INSERT INTO lawndart.event_types (token) VALUES (@token) ON CONFLICT (token) DO NOTHING", connection);
            command.Parameters.AddWithValue("token", name);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var map = new Dictionary<string, int>(StringComparer.Ordinal);
        await using var read = new NpgsqlCommand("SELECT id, token FROM lawndart.event_types", connection);
        await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            map[reader.GetString(1)] = reader.GetInt32(0);
        return map;
    }

    private static async Task<Dictionary<string, int>> InsertSqlTypesAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        foreach (var name in EventGenerator.EventTypeNames)
        {
            await using var command = new SqlCommand("""
                IF NOT EXISTS (SELECT 1 FROM dbo.EventTypes WHERE Token = @token)
                    INSERT INTO dbo.EventTypes (Token) VALUES (@token);
                """, connection);
            command.Parameters.AddWithValue("@token", name);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var map = new Dictionary<string, int>(StringComparer.Ordinal);
        await using var read = new SqlCommand("SELECT Id, Token FROM dbo.EventTypes", connection);
        await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            map[reader.GetString(1)] = reader.GetInt32(0);
        return map;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
    }

    private static async Task ExecSqlAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 0 };
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<long> ScalarSqlAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 0 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
    }
}

internal sealed class ObjectReader : IDataReader
{
    private readonly string[] _names;
    private readonly Type[] _types;
    private readonly IReadOnlyList<object?[]> _rows;
    private int _index = -1;

    public ObjectReader(string[] names, Type[] types, IReadOnlyList<object?[]> rows)
    {
        _names = names;
        _types = types;
        _rows = rows;
    }

    public int FieldCount => _names.Length;
    public int Depth => 0;
    public bool IsClosed => false;
    public int RecordsAffected => -1;
    public object this[int i] => GetValue(i);
    public object this[string name] => GetValue(GetOrdinal(name));

    public bool Read()
    {
        _index++;
        return _index < _rows.Count;
    }

    public object GetValue(int i)
    {
        var value = _rows[_index][i];
        return value ?? DBNull.Value;
    }

    public int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    public bool IsDBNull(int i) => GetValue(i) is DBNull;
    public string GetName(int i) => _names[i];
    public Type GetFieldType(int i) => _types[i];
    public string GetDataTypeName(int i) => _types[i].Name;
    public int GetOrdinal(string name)
    {
        for (var i = 0; i < _names.Length; i++)
        {
            if (_names[i].Equals(name, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        throw new IndexOutOfRangeException(name);
    }

    public bool NextResult() => false;
    public void Close() { }
    public DataTable? GetSchemaTable() => null;
    public void Dispose() { }
    public bool GetBoolean(int i) => throw new NotSupportedException();
    public byte GetByte(int i) => Convert.ToByte(GetValue(i), CultureInfo.InvariantCulture);
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
    {
        var data = (byte[])GetValue(i);
        if (buffer is null)
            return data.Length;
        var count = Math.Min(length, data.Length - (int)fieldOffset);
        Array.Copy(data, (int)fieldOffset, buffer, bufferoffset, count);
        return count;
    }

    public char GetChar(int i) => throw new NotSupportedException();
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
    public IDataReader GetData(int i) => throw new NotSupportedException();
    public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
    public decimal GetDecimal(int i) => Convert.ToDecimal(GetValue(i), CultureInfo.InvariantCulture);
    public double GetDouble(int i) => Convert.ToDouble(GetValue(i), CultureInfo.InvariantCulture);
    public float GetFloat(int i) => Convert.ToSingle(GetValue(i), CultureInfo.InvariantCulture);
    public Guid GetGuid(int i) => (Guid)GetValue(i);
    public short GetInt16(int i) => Convert.ToInt16(GetValue(i), CultureInfo.InvariantCulture);
    public int GetInt32(int i) => Convert.ToInt32(GetValue(i), CultureInfo.InvariantCulture);
    public long GetInt64(int i) => Convert.ToInt64(GetValue(i), CultureInfo.InvariantCulture);
    public string GetString(int i) => (string)GetValue(i);
}

internal static class ShapeProof
{
    public static async Task AssertAsync(IEventLog log, ISnapshotStore snapshots, DatasetCatalog catalog, CancellationToken cancellationToken)
    {
        var hot = await log.ReadStreamAsync(catalog.HotStreamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (hot.Count != catalog.HotVersion)
            throw new InvalidOperationException("The hot stream does not have its generated events.");

        if (catalog.Streams.Count < 1)
            throw new InvalidOperationException("The catalog has no stream to load.");

        var sample = catalog.Streams[0];
        var loaded = await snapshots.LoadSnapshotAsync<BenchState>(sample.Id, cancellationToken).ConfigureAwait(false);
        if (loaded.State is null || loaded.Info is null || loaded.Info.Version != sample.SnapshotVersion)
            throw new InvalidOperationException("The snapshot did not load.");

        var tail = await log.ReadStreamAsync(sample.Id, sample.SnapshotVersion + 1, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (tail.Count != sample.Version - sample.SnapshotVersion)
            throw new InvalidOperationException("The stream tail does not match the snapshot.");

        var query = await log.ReadByQueryAsync(
            Query.FromItems(QueryItem.ByTypeAndTags([EventGenerator.QueryType], [EventGenerator.QueryTag])),
            limit: 1,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        if (query.Events.Count < 1)
            throw new InvalidOperationException("A tag and type query returned no event.");
    }
}
