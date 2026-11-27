using System.Globalization;
using System.Text;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// DDL for one context. Running it twice is a no-op when the stamp matches.
/// The outbox table is a separate <c>schema_info</c> component and does not change that number.
/// </summary>
/// <remarks>
/// Format 7 keys events on position. With position ranges on, events and event_tags are
/// split by position range, one child table per range. Event ids are not unique.
/// </remarks>
internal static class PostgresSchema
{
    internal const int CurrentFormat = 7;

    internal const string EventsComponent = "events";

    /// <summary>Empty ranges kept above the range that holds the newest position.</summary>
    internal const int RangesAhead = 2;

    /// <summary>Marker written into the range function so a schema check can tell its body.</summary>
    internal const string EnsureMarkerPrefix = "LawnDart:ensure_event_partitions:";

    internal const int EnsureMarkerVersion = 1;

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
            SchemaInfoTable(names.SchemaInfo)
        };

        var ranges = options.UsesPositionPartitions;
        sql.Add(CreateEventTable(names.Events, EventsColumns(names), "PRIMARY KEY (global_position)", ranges));
        sql.Add(EnableLz4(names));
        sql.Add(EventIndexes(names, options));
        sql.Add(CreateEventTable(names.Tags, TagsColumns(names), "PRIMARY KEY (tag, global_position, tenant_id) INCLUDE (event_type_id)", ranges, TagStorage));
        sql.Add($"CREATE TABLE IF NOT EXISTS {names.Streams} ({StreamsColumns(names)}, {StreamsKeys()})");
        sql.Add($"CREATE TABLE IF NOT EXISTS {names.StreamTags} ({StreamTagsColumns(names)}, {StreamTagsKeys()})");
        sql.Add(EnsurePartitionsFunction(names, options));
        if (ranges)
            sql.Add($"SELECT {names.EnsurePartitions}((SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM {names.Sequence}), {RangesAhead})");
        sql.Add(DropAppendFunction(names));
        sql.Add(PostgresAppendSql.CreateFunction(names, options));
        sql.Add(Stamp(names, options));
        return sql;
    }

    /// <summary>
    /// The stamp table. The inbox and the outbox writer create the same shape.
    /// <c>partition_size</c> is the events range size, or null when ranges are off.
    /// </summary>
    internal static string SchemaInfoTable(string schemaInfo) => $"""
        CREATE TABLE IF NOT EXISTS {schemaInfo} (
            component text PRIMARY KEY,
            format int NOT NULL,
            partition_count int NOT NULL,
            partitioning boolean NOT NULL,
            updated_at timestamptz NOT NULL,
            partition_size bigint NULL
        );
        DO $add$
        BEGIN
            -- Only an older table lacks the column. Checking first means a role that
            -- does not own the table, and the table lock, are only needed then.
            IF NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_attribute
                WHERE attrelid = '{Literal(schemaInfo)}'::regclass AND attname = 'partition_size' AND NOT attisdropped
            ) THEN
                ALTER TABLE {schemaInfo} ADD COLUMN IF NOT EXISTS partition_size bigint NULL;
            END IF;
        END $add$
        """;

    // Fixed width columns first, so rows carry less padding.
    private static string EventsColumns(PostgresNames names) => $"""
        global_position bigint NOT NULL,
        version bigint NOT NULL,
        recorded_at timestamptz NOT NULL,
        event_type_id int NOT NULL,
        schema_version int NOT NULL,
        partition_hash int NOT NULL,
        codec_id smallint NOT NULL,
        event_id uuid NOT NULL,
        tenant_id {names.ExactText} NOT NULL CHECK (tenant_id <> ''),
        stream_id {names.ExactText} NOT NULL,
        data jsonb NULL,
        bdata bytea NULL,
        metadata jsonb NULL,
        tags {names.ExactText}[] NULL,
        CHECK ((data IS NULL) <> (bdata IS NULL))
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
        global_position bigint NOT NULL,
        event_type_id int NOT NULL,
        tag {names.ExactText} NOT NULL,
        tenant_id {names.ExactText} NOT NULL CHECK (tenant_id <> '')
        """;

    /// <summary>
    /// Rows are written once and never updated, so vacuum runs for inserts often enough
    /// to keep the visibility map current for index-only reads.
    /// </summary>
    internal const string InsertOnlyStorage =
        "autovacuum_vacuum_insert_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.02, fillfactor = 100";

    /// <summary>
    /// Tag reads are short and stop at a page of results. When all of a tag's rows sit in
    /// one range, the planner can pick a parallel plan whose worker start-up costs far more
    /// than the read itself, so tag tables never use parallel workers.
    /// </summary>
    internal const string TagStorage = InsertOnlyStorage + ", parallel_workers = 0";

    /// <summary>
    /// With ranges on, the parent holds no rows; each range table gets the storage settings
    /// when it is created. Without ranges, the one table gets them here.
    /// </summary>
    private static string CreateEventTable(string table, string columns, string key, bool ranges, string storage = InsertOnlyStorage)
        => ranges
            ? $"CREATE TABLE IF NOT EXISTS {table} ({columns}, {key}) PARTITION BY RANGE (global_position)"
            : $"CREATE TABLE IF NOT EXISTS {table} ({columns}, {key}) WITH ({storage})";

    /// <summary>
    /// Stream reads, type reads, tenant reads, and time windows. No event index is unique,
    /// and event ids have no index.
    /// The tenant index is only built when many tenants share the tables.
    /// </summary>
    private static string EventIndexes(PostgresNames names, PostgresEventStoreOptions options)
    {
        var sql = new StringBuilder();
        sql.Append("CREATE INDEX IF NOT EXISTS ix_events_stream ON ").Append(names.Events).Append(" (tenant_id, stream_id, version);\n");
        sql.Append("CREATE INDEX IF NOT EXISTS ix_events_type ON ").Append(names.Events).Append(" (event_type_id, global_position);\n");
        if (options.Tenancy == Tenancy.Shared)
            sql.Append("CREATE INDEX IF NOT EXISTS ix_events_tenant ON ").Append(names.Events).Append(" (tenant_id, global_position);\n");
        sql.Append("CREATE INDEX IF NOT EXISTS ix_events_recorded_at ON ").Append(names.Events).Append(" USING brin (recorded_at) WITH (autosummarize = on);");
        return sql.ToString();
    }

    /// <summary>
    /// Uses lz4 for payload and metadata when the server was built with it.
    /// Runs only while the column still has another method, so a second setup takes no lock.
    /// </summary>
    private static string EnableLz4(PostgresNames names)
    {
        var events = names.Events.Replace("'", "''", StringComparison.Ordinal);
        var alter = ("ALTER TABLE " + names.Events
            + " ALTER COLUMN data SET COMPRESSION lz4, ALTER COLUMN bdata SET COMPRESSION lz4, ALTER COLUMN metadata SET COMPRESSION lz4")
            .Replace("'", "''", StringComparison.Ordinal);
        return $"""
            DO $lz4$
            BEGIN
                IF EXISTS (
                       SELECT 1 FROM pg_settings
                       WHERE name = 'default_toast_compression' AND 'lz4' = ANY(enumvals))
                   AND EXISTS (
                       SELECT 1 FROM pg_attribute
                       WHERE attrelid = to_regclass('{events}')
                         AND attname IN ('data', 'bdata', 'metadata')
                         AND attcompression <> 'l') THEN
                    EXECUTE '{alter}';
                END IF;
            END
            $lz4$;
            """;
    }

    /// <summary>
    /// Creates the events and event_tags range tables up to the range that holds
    /// <c>p_position</c> (or the newest position handed out, when null), plus <c>p_ahead</c> more (at most 2),
    /// and returns the first position with no range.
    /// </summary>
    /// <remarks>
    /// A new range is created as a plain table and then attached. Attaching takes a lock
    /// on the parent that appends and reads do not wait for; creating it directly as a
    /// partition would lock out every append and read until the work is done.
    /// When every range it needs exists, the function reads the catalog and takes no lock.
    /// Otherwise one caller at a time does the work under an advisory lock, so two
    /// writers or two nodes never create the same range. Ranges below the highest one
    /// are never created again, so a range moved out later stays out. It works in a
    /// repeatable read or serializable transaction too.
    /// Without position ranges the function returns null and does nothing.
    /// </remarks>
    internal static string EnsurePartitionsFunction(PostgresNames names, PostgresEventStoreOptions options)
    {
        var ranges = options.UsesPositionPartitions;
        var size = options.PartitionSize.ToString(CultureInfo.InvariantCulture);
        var schema = Literal(names.Schema);
        var eventsTable = Literal(names.EventsTable);
        var tagsTable = Literal(names.TagsTable);
        var events = Literal(names.Events);
        var tags = Literal(names.Tags);
        var lockKey = Literal(names.Schema + ":partitions");
        var storage = Literal(InsertOnlyStorage);
        var tagStorage = Literal(TagStorage);
        var sequence = names.Sequence;
        var body = ranges
            ? $"""
                v_last := (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM {sequence});
                -- Never past the newest position handed out, so a caller cannot ask for
                -- ranges far ahead of the data.
                v_target := (greatest(least(coalesce(p_position, v_last), v_last), 1) - 1) / v_size
                    + least(greatest(coalesce(p_ahead, 0), 0), 2);
                v_max := (
                    SELECT pg_catalog.max(pg_catalog.substr(c.relname, pg_catalog.length(v_prefix) + 1)::bigint)
                    FROM pg_catalog.pg_inherits i
                    JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
                    WHERE i.inhparent = pg_catalog.to_regclass('{events}')
                      AND pg_catalog.left(c.relname, pg_catalog.length(v_prefix)) = v_prefix
                      AND pg_catalog.substr(c.relname, pg_catalog.length(v_prefix) + 1) ~ '^[0-9]+$');
                IF v_max IS NOT NULL AND v_max >= v_target THEN
                    RETURN (v_max + 1) * v_size + 1;
                END IF;

                PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('{lockKey}', 0));
                v_max := (
                    SELECT pg_catalog.max(pg_catalog.substr(c.relname, pg_catalog.length(v_prefix) + 1)::bigint)
                    FROM pg_catalog.pg_inherits i
                    JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
                    WHERE i.inhparent = pg_catalog.to_regclass('{events}')
                      AND pg_catalog.left(c.relname, pg_catalog.length(v_prefix)) = v_prefix
                      AND pg_catalog.substr(c.relname, pg_catalog.length(v_prefix) + 1) ~ '^[0-9]+$');
                -- In a repeatable read or serializable transaction the read above still
                -- uses the snapshot from before the lock, so a range another caller just
                -- made can look missing. Each table is looked up by name first, which
                -- sees committed tables, and a table that appears meanwhile counts as made.
                v_k := coalesce(v_max + 1, 0);
                WHILE v_k <= v_target LOOP
                    v_low := v_k * v_size + 1;
                    v_high := (v_k + 1) * v_size + 1;
                    IF pg_catalog.to_regclass(pg_catalog.format('%I.%I', '{schema}', '{eventsTable}_p' || v_k)) IS NULL THEN
                        BEGIN
                            EXECUTE pg_catalog.format(
                                'CREATE TABLE %I.%I (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION) WITH (%s)',
                                '{schema}', '{eventsTable}_p' || v_k, '{events}', '{storage}');
                            EXECUTE pg_catalog.format(
                                'ALTER TABLE %s ATTACH PARTITION %I.%I FOR VALUES FROM (%s) TO (%s)',
                                '{events}', '{schema}', '{eventsTable}_p' || v_k, v_low, v_high);
                        EXCEPTION WHEN duplicate_table OR invalid_object_definition THEN
                            NULL;
                        END;
                    END IF;
                    IF pg_catalog.to_regclass(pg_catalog.format('%I.%I', '{schema}', '{tagsTable}_p' || v_k)) IS NULL THEN
                        BEGIN
                            EXECUTE pg_catalog.format(
                                'CREATE TABLE %I.%I (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION) WITH (%s)',
                                '{schema}', '{tagsTable}_p' || v_k, '{tags}', '{tagStorage}');
                            EXECUTE pg_catalog.format(
                                'ALTER TABLE %s ATTACH PARTITION %I.%I FOR VALUES FROM (%s) TO (%s)',
                                '{tags}', '{schema}', '{tagsTable}_p' || v_k, v_low, v_high);
                        EXCEPTION WHEN duplicate_table OR invalid_object_definition THEN
                            NULL;
                        END;
                    END IF;
                    v_k := v_k + 1;
                END LOOP;
                RETURN (greatest(coalesce(v_max, -1), v_target) + 1) * v_size + 1;
            """
            : """
                RETURN NULL;
            """;
        // Every role that appends may need a range, so EXECUTE is granted to PUBLIC.
        // Calling it still needs USAGE on the schema. The function runs as the owner,
        // with a fixed search path, and only makes ranges up to a few above the
        // newest position handed out, so a call cannot do more than an append would.
        return $"""
            CREATE OR REPLACE FUNCTION {names.EnsurePartitions}(p_position bigint, p_ahead int)
            RETURNS bigint
            LANGUAGE plpgsql
            SECURITY DEFINER
            SET search_path = pg_catalog, pg_temp
            AS $fn$
            DECLARE
                v_size constant bigint := {size};
                v_prefix constant text := '{eventsTable}_p';
                v_target bigint;
                v_max bigint;
                v_k bigint;
                v_low bigint;
                v_high bigint;
                v_last bigint;
            BEGIN
                -- {EnsureMarkerPrefix}{EnsureMarkerVersion.ToString(CultureInfo.InvariantCulture)}
            {body}
            END;
            $fn$;
            REVOKE ALL ON FUNCTION {names.EnsurePartitions}(bigint, int) FROM PUBLIC;
            GRANT EXECUTE ON FUNCTION {names.EnsurePartitions}(bigint, int) TO PUBLIC;
            """;
    }

    internal static string ExpectedEnsureProsrc(PostgresNames names, PostgresEventStoreOptions options)
        => PostgresSchemaProbe.DollarBody(EnsurePartitionsFunction(names, options), "$fn$");

    private static string Literal(string value) => value.Replace("'", "''", StringComparison.Ordinal);

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
        _ = options;
        return $"""
            INSERT INTO {names.SchemaInfo} (component, format, partition_count, partitioning, updated_at)
            VALUES ('{StreamTagsComponent}', 1, 0, false, clock_timestamp())
            ON CONFLICT (component) DO NOTHING
            """;
    }

    /// <summary>
    /// Stamps the format and the range setting. <c>partition_count</c> is 0 in this format;
    /// <c>partitioning</c> means position ranges, and <c>partition_size</c> is their size.
    /// </summary>
    private static string Stamp(PostgresNames names, PostgresEventStoreOptions options)
    {
        var ranges = options.UsesPositionPartitions;
        var size = ranges ? options.PartitionSize.ToString(CultureInfo.InvariantCulture) : "NULL";
        return $"""
            INSERT INTO {names.SchemaInfo} (component, format, partition_count, partitioning, partition_size, updated_at)
            VALUES ('{EventsComponent}', {CurrentFormat}, 0, {(ranges ? "true" : "false")}, {size}, clock_timestamp())
            ON CONFLICT (component) DO NOTHING
            """;
    }
}
