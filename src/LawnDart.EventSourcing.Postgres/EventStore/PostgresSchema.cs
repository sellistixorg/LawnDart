using System.Globalization;
using System.Text;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// DDL for one context. Format 1. Running it twice is a no-op when the stamp matches.
/// </summary>
internal static class PostgresSchema
{
    internal const int CurrentFormat = 1;

    internal static IReadOnlyList<string> CreateStatements(PostgresNames names, PostgresEventStoreOptions options)
    {
        var sql = new List<string>
        {
            $"CREATE SCHEMA IF NOT EXISTS {PgIdentifier.Quote(names.Schema)}",
            $"""
            DO $$ BEGIN
                CREATE DOMAIN {names.ExactText} AS text COLLATE "C";
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;
            """,
            $"CREATE SEQUENCE IF NOT EXISTS {names.Sequence} AS bigint INCREMENT BY 1 MINVALUE 1 START WITH 1 CACHE 1",
            $"""
            CREATE TABLE IF NOT EXISTS {names.Types} (
                id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                token text NOT NULL UNIQUE
            )
            """,
            $"""
            CREATE TABLE IF NOT EXISTS {names.SchemaInfo} (
                component text PRIMARY KEY,
                format int NOT NULL,
                partition_count int NOT NULL,
                partitioning boolean NOT NULL,
                updated_at timestamptz NOT NULL
            )
            """
        };

        sql.Add(CreatePartitioned(names.Events, names.EventsShared, names.EventsHash, options, EventsColumns(names), EventsKeys()));
        sql.Add(CreatePartitioned(names.Streams, names.StreamsShared, names.StreamsHash, options, StreamsColumns(names), StreamsKeys()));
        sql.Add(CreatePartitioned(names.Tags, names.TagsShared, names.TagsHash, options, TagsColumns(names), TagsKeys()));
        sql.Add(PostgresAppendSql.CreateFunction(names, options));
        sql.Add(Stamp(names, options));
        return sql;
    }

    private static string EventsColumns(PostgresNames names) => $"""
        tenant_id {names.ExactText} NOT NULL CHECK (tenant_id <> ''),
        stream_id {names.ExactText} NOT NULL,
        version bigint NOT NULL,
        global_position bigint NOT NULL,
        event_id uuid NOT NULL,
        event_type_id int NOT NULL REFERENCES {names.Types} (id),
        schema_version int NOT NULL,
        codec_id smallint NOT NULL,
        data jsonb NULL,
        bdata bytea NULL,
        metadata jsonb NULL,
        tags {names.ExactText}[] NULL,
        partition_hash int NOT NULL,
        recorded_at timestamptz NOT NULL,
        CHECK ((data IS NULL) <> (bdata IS NULL))
        """;

    private static string EventsKeys() => """
        PRIMARY KEY (tenant_id, stream_id, version),
        UNIQUE (global_position, tenant_id),
        UNIQUE (event_id, tenant_id)
        """;

    private static string StreamsColumns(PostgresNames names) => $"""
        tenant_id {names.ExactText} NOT NULL CHECK (tenant_id <> ''),
        stream_id {names.ExactText} NOT NULL,
        aggregate_type text NOT NULL,
        aggregate_id uuid NULL,
        current_version bigint NOT NULL,
        last_global_position bigint NOT NULL,
        created_at timestamptz NOT NULL,
        last_event_at timestamptz NOT NULL,
        event_count bigint NOT NULL,
        tags {names.ExactText}[] NULL,
        status text NOT NULL DEFAULT 'Active'
        """;

    private static string StreamsKeys() => "PRIMARY KEY (tenant_id, stream_id)";

    private static string TagsColumns(PostgresNames names) => $"""
        tag {names.ExactText} NOT NULL,
        global_position bigint NOT NULL,
        tenant_id {names.ExactText} NOT NULL CHECK (tenant_id <> '')
        """;

    private static string TagsKeys() => "PRIMARY KEY (tag, global_position, tenant_id)";

    private static string CreatePartitioned(
        string table,
        string shared,
        Func<int, string> hashName,
        PostgresEventStoreOptions options,
        string columns,
        string keys)
    {
        var builder = new StringBuilder();
        if (!options.Partitioning)
        {
            builder.Append("CREATE TABLE IF NOT EXISTS ").Append(table).Append(" (").Append(columns).Append(", ").Append(keys).Append(')');
            return builder.ToString();
        }

        builder.Append("CREATE TABLE IF NOT EXISTS ").Append(table).Append(" (").Append(columns).Append(", ").Append(keys).Append(") PARTITION BY LIST (tenant_id);");
        builder.Append("CREATE TABLE IF NOT EXISTS ").Append(shared).Append(" PARTITION OF ").Append(table).Append(" DEFAULT PARTITION BY HASH (tenant_id);");
        for (var i = 0; i < options.PartitionCount; i++)
        {
            builder.Append("CREATE TABLE IF NOT EXISTS ").Append(hashName(i))
                .Append(" PARTITION OF ").Append(shared)
                .Append(" FOR VALUES WITH (MODULUS ")
                .Append(options.PartitionCount.ToString(CultureInfo.InvariantCulture))
                .Append(", REMAINDER ")
                .Append(i.ToString(CultureInfo.InvariantCulture))
                .Append(");");
        }

        return builder.ToString();
    }

    internal static string StreamIndexes(PostgresNames names) => $"""
        CREATE INDEX IF NOT EXISTS ix_streams_aggregate_type ON {names.Streams} (aggregate_type);
        CREATE INDEX IF NOT EXISTS ix_streams_last_position ON {names.Streams} (last_global_position);
        CREATE INDEX IF NOT EXISTS ix_streams_stream_id ON {names.Streams} (stream_id);
        CREATE INDEX IF NOT EXISTS ix_streams_tags ON {names.Streams} USING GIN (tags);
        """;

    private static string Stamp(PostgresNames names, PostgresEventStoreOptions options)
    {
        var count = options.PartitionCount.ToString(CultureInfo.InvariantCulture);
        var partitioning = options.Partitioning ? "true" : "false";
        return $"""
            INSERT INTO {names.SchemaInfo} (component, format, partition_count, partitioning, updated_at)
            VALUES ('events', {CurrentFormat}, {count}, {partitioning}, clock_timestamp())
            ON CONFLICT (component) DO NOTHING
            """;
    }
}
