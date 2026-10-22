using System.Globalization;
using System.Text;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// DDL for one context. The events format stays at 1. Running it twice is a no-op when the stamp matches.
/// The outbox table is a separate <c>schema_info</c> component and does not change that number.
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
        sql.Add(CreatePartitioned(names.StreamTags, names.StreamTagsShared, names.StreamTagsHash, options, StreamTagsColumns(names), StreamTagsKeys()));
        sql.Add(CreatePartitioned(names.Tags, names.TagsShared, names.TagsHash, options, TagsColumns(names), TagsKeys()));
        sql.Add(DropAppendFunction(names));
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

    private static string StreamTagsColumns(PostgresNames names) => $"""
        tenant_id {names.ExactText} NOT NULL CHECK (tenant_id <> ''),
        tag {names.ExactText} NOT NULL CHECK (tag <> ''),
        stream_id {names.ExactText} NOT NULL
        """;

    private static string StreamTagsKeys() => "PRIMARY KEY (tenant_id, tag, stream_id)";

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

    private static string DropAppendFunction(PostgresNames names) => $"""
        DO $drop$
        DECLARE sig regprocedure;
        BEGIN
            FOR sig IN
                SELECT p.oid::regprocedure
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE p.proname = 'append_to_stream' AND n.nspname = '{names.Schema.Replace("'", "''", StringComparison.Ordinal)}'
            LOOP
                EXECUTE 'DROP FUNCTION ' || sig::text;
            END LOOP;
        END
        $drop$;
        """;

    internal static string StreamIndexes(PostgresNames names) => $"""
        CREATE INDEX IF NOT EXISTS ix_streams_aggregate_type ON {names.Streams} (aggregate_type, stream_id);
        CREATE INDEX IF NOT EXISTS ix_streams_last_position ON {names.Streams} (last_global_position);
        CREATE INDEX IF NOT EXISTS ix_streams_stream_id ON {names.Streams} (stream_id);
        CREATE INDEX IF NOT EXISTS ix_streams_tags ON {names.Streams} USING GIN (tags);
        CREATE INDEX IF NOT EXISTS ix_stream_tags_tag_stream ON {names.StreamTags} (tag, stream_id);
        """;

    /// <summary>
    /// Replaces an aggregate type index that does not include <c>stream_id</c>.
    /// A new database already has the two-column index, so this is a no-op there.
    /// </summary>
    internal static string EnsureAggregateIndex(PostgresNames names)
    {
        var schema = names.Schema.Replace("'", "''", StringComparison.Ordinal);
        var drop = ("DROP INDEX IF EXISTS " + PgIdentifier.Qualify(names.Schema, "ix_streams_aggregate_type"))
            .Replace("'", "''", StringComparison.Ordinal);
        var create = ("CREATE INDEX ix_streams_aggregate_type ON " + names.Streams + " (aggregate_type, stream_id)")
            .Replace("'", "''", StringComparison.Ordinal);
        return $"""
            DO $idx$
            DECLARE def text;
            BEGIN
                SELECT indexdef INTO def
                FROM pg_indexes
                WHERE schemaname = '{schema}' AND indexname = 'ix_streams_aggregate_type';
                IF def IS NULL OR position('stream_id' in def) = 0 THEN
                    EXECUTE '{drop}';
                    EXECUTE '{create}';
                END IF;
            END
            $idx$;
            """;
    }

    internal const string StreamTagsComponent = "stream_tags";
    internal const int StreamTagBatchSize = 2000;

    /// <summary>
    /// Keeps <c>stream_tags</c> aligned with <c>streams.tags</c> on insert, tag update, and delete.
    /// Replacing the append function does not remove this trigger.
    /// </summary>
    internal static string StreamTagTriggerFunction(PostgresNames names) => $"""
        CREATE OR REPLACE FUNCTION {PgIdentifier.Qualify(names.Schema, "sync_stream_tags")}()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $fn$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                DELETE FROM {names.StreamTags}
                WHERE tenant_id = OLD.tenant_id AND stream_id = OLD.stream_id;
                RETURN OLD;
            END IF;

            IF TG_OP = 'UPDATE'
               AND (OLD.tenant_id IS DISTINCT FROM NEW.tenant_id OR OLD.stream_id IS DISTINCT FROM NEW.stream_id) THEN
                DELETE FROM {names.StreamTags}
                WHERE tenant_id = OLD.tenant_id AND stream_id = OLD.stream_id;
            END IF;

            DELETE FROM {names.StreamTags} AS st
            WHERE st.tenant_id = NEW.tenant_id
              AND st.stream_id = NEW.stream_id
              AND st.tag <> ALL (
                  SELECT u.tag
                  FROM unnest(coalesce(NEW.tags, ARRAY[]::text[])) AS u(tag)
                  WHERE u.tag IS NOT NULL
              );

            INSERT INTO {names.StreamTags} (tenant_id, tag, stream_id)
            SELECT NEW.tenant_id, u.tag, NEW.stream_id
            FROM unnest(coalesce(NEW.tags, ARRAY[]::text[])) AS u(tag)
            WHERE u.tag IS NOT NULL AND length(u.tag) > 0
            ON CONFLICT DO NOTHING;

            RETURN NEW;
        END;
        $fn$;
        """;

    internal static string EnsureStreamTagTrigger(PostgresNames names)
    {
        var function = PgIdentifier.Qualify(names.Schema, "sync_stream_tags");
        var relation = names.Streams.Replace("'", "''", StringComparison.Ordinal);
        var create = $"""
            CREATE TRIGGER streams_sync_tags
            AFTER INSERT OR DELETE OR UPDATE OF tags
            ON {names.Streams}
            FOR EACH ROW
            EXECUTE FUNCTION {function}()
            """.Replace("'", "''", StringComparison.Ordinal);
        return $"""
            DO $trg$
            BEGIN
                IF to_regclass('{relation}') IS NOT NULL
                   AND NOT EXISTS (
                        SELECT 1
                        FROM pg_trigger
                        WHERE NOT tgisinternal
                          AND tgname = 'streams_sync_tags'
                          AND tgrelid = to_regclass('{relation}')) THEN
                    EXECUTE '{create}';
                END IF;
            END
            $trg$;
            """;
    }

    /// <summary>
    /// Copies tags for the next batch of streams after <c>@tenant</c> and <c>@stream</c>.
    /// An empty pair is before every stored tenant. Returns the last stream in that batch. No row means the copy is finished.
    /// </summary>
    internal static string BackfillStreamTagsBatch(PostgresNames names) => $"""
        WITH batch AS (
            SELECT s.tenant_id, s.stream_id, s.tags
            FROM {names.Streams} AS s
            WHERE s.tenant_id > @tenant
               OR (s.tenant_id = @tenant AND s.stream_id > @stream)
            ORDER BY s.tenant_id, s.stream_id
            LIMIT {StreamTagBatchSize.ToString(CultureInfo.InvariantCulture)}
        ),
        copied AS (
            INSERT INTO {names.StreamTags} (tenant_id, tag, stream_id)
            SELECT b.tenant_id, u.tag, b.stream_id
            FROM batch AS b
            CROSS JOIN LATERAL unnest(coalesce(b.tags, ARRAY[]::text[])) AS u(tag)
            WHERE u.tag IS NOT NULL AND length(u.tag) > 0
            ON CONFLICT DO NOTHING
            RETURNING 1
        )
        SELECT tenant_id, stream_id
        FROM batch
        WHERE (SELECT count(*) FROM copied) >= 0
        ORDER BY tenant_id DESC, stream_id DESC
        LIMIT 1
        """;

    internal static string StreamTagsMarkerExists(PostgresNames names) =>
        $"SELECT 1 FROM {names.SchemaInfo} WHERE component = '{StreamTagsComponent}'";

    internal static string InsertStreamTagsMarker(PostgresNames names, PostgresEventStoreOptions options)
    {
        var count = options.PartitionCount.ToString(CultureInfo.InvariantCulture);
        var partitioning = options.Partitioning ? "true" : "false";
        return $"""
            INSERT INTO {names.SchemaInfo} (component, format, partition_count, partitioning, updated_at)
            VALUES ('{StreamTagsComponent}', 1, {count}, {partitioning}, clock_timestamp())
            ON CONFLICT (component) DO NOTHING
            """;
    }

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
