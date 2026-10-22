namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// One function call appends a batch. The version check runs before any
/// sequence value is taken, so a losing writer does not burn positions.
/// </summary>
internal static class PostgresAppendSql
{
    /// <summary>Prefix of the version marker written into <c>append_to_stream</c>.</summary>
    internal const string BodyMarkerPrefix = "LawnDart:append_to_stream:";

    /// <summary>Marker version written into <c>append_to_stream</c>. A later number is a different body.</summary>
    internal const int BodyMarkerVersion = 1;

    /// <summary>Text written into <c>append_to_stream</c> so a later read can tell this body from an older one.</summary>
    internal const string BodyMarker = BodyMarkerPrefix + "1";

    internal static string ExpectedProsrc(PostgresNames names, PostgresEventStoreOptions options)
        => PostgresSchemaProbe.DollarBody(CreateFunction(names, options), "$fn$");

    internal static string CreateFunction(PostgresNames names, PostgresEventStoreOptions options)
    {
        _ = options;
        return $"""
            CREATE OR REPLACE FUNCTION {names.Append}(
                p_stream_id text,
                p_tenant_id text,
                p_expected bigint,
                p_is_dcb boolean,
                p_aggregate_type text,
                p_aggregate_id uuid,
                p_event_ids uuid[],
                p_type_ids int[],
                p_schema_versions int[],
                p_codec_ids smallint[],
                p_data jsonb[],
                p_bdata bytea[],
                p_metadata jsonb[],
                p_tag_ords int[],
                p_tag_values text[],
                p_stream_tags text[],
                p_partition_hash int,
                p_recorded_at timestamptz,
                p_notify boolean,
                p_notify_channel text,
                p_lock_tags text[],
                p_lock_modes text[],
                p_lock_store text,
                p_store_lock_key text,
                p_check_condition boolean,
                p_condition_all boolean,
                p_condition_after bigint,
                p_item_count int,
                p_type_items int[],
                p_type_ids_cond int[],
                p_tag_items int[],
                p_tag_values_cond text[],
                p_items_with_types int[],
                p_items_with_tags int[]
            ) RETURNS TABLE (global_position bigint, version bigint)
            LANGUAGE plpgsql
            SET search_path = pg_catalog
            AS $fn$
            DECLARE
                v_current bigint;
                v_count bigint;
                v_actual bigint;
                v_start bigint;
                v_n int;
                v_positions bigint[];
                v_i int;
                v_constraint text;
                v_detail text;
                v_baseline bigint;
            BEGIN
                -- {BodyMarker}
                -- Assign a transaction id before any position is taken.
                -- A reader that sees a later position can then prove which
                -- writers might still fill a hole: they already had an id.
                PERFORM pg_current_xact_id();

                -- Store key first, then tags in the order the caller sorted.
                -- Every append uses that same order, so opposite tag lists cannot deadlock.
                -- A dynamic consistency boundary append holds exclusive locks on its
                -- condition tags and shared locks on the tags it writes, plus the store key.
                IF p_lock_store = 'exclusive' THEN
                    PERFORM pg_advisory_xact_lock(hashtextextended(p_store_lock_key, 0));
                ELSE
                    PERFORM pg_advisory_xact_lock_shared(hashtextextended(p_store_lock_key, 0));
                END IF;

                IF p_lock_tags IS NOT NULL THEN
                    FOR v_i IN 1 .. coalesce(array_length(p_lock_tags, 1), 0) LOOP
                        IF p_lock_modes[v_i] = 'exclusive' THEN
                            PERFORM pg_advisory_xact_lock(hashtextextended(p_store_lock_key || chr(31) || p_lock_tags[v_i], 0));
                        ELSE
                            PERFORM pg_advisory_xact_lock_shared(hashtextextended(p_store_lock_key || chr(31) || p_lock_tags[v_i], 0));
                        END IF;
                    END LOOP;
                END IF;

                -- The condition runs in its own statement, so it sees every
                -- writer that committed before these locks were acquired.
                IF p_check_condition THEN
                    IF p_condition_all THEN
                        IF EXISTS (
                            SELECT 1 FROM {names.Events} e
                            WHERE p_condition_after IS NULL OR e.global_position > p_condition_after
                        ) THEN
                            RAISE EXCEPTION 'condition' USING ERRCODE = 'P0001', HINT = 'concurrency', DETAIL = coalesce(p_condition_after::text, '');
                        END IF;
                    ELSIF coalesce(p_item_count, 0) > 0 THEN
                        FOR v_i IN 1 .. p_item_count LOOP
                            IF EXISTS (
                                SELECT 1 FROM {names.Events} e
                                WHERE (p_condition_after IS NULL OR e.global_position > p_condition_after)
                                  AND (
                                      NOT (v_i = ANY(coalesce(p_items_with_types, ARRAY[]::int[])))
                                      OR e.event_type_id IN (
                                          SELECT t.id FROM unnest(p_type_items, p_type_ids_cond) AS t(item, id) WHERE t.item = v_i
                                      )
                                  )
                                  AND (
                                      NOT (v_i = ANY(coalesce(p_items_with_tags, ARRAY[]::int[])))
                                      OR NOT EXISTS (
                                          SELECT 1
                                          FROM unnest(p_tag_items, p_tag_values_cond) AS tg(item, tag)
                                          WHERE tg.item = v_i
                                            AND NOT EXISTS (
                                                SELECT 1 FROM {names.Tags} et
                                                WHERE et.global_position = e.global_position
                                                  AND et.tenant_id = e.tenant_id
                                                  AND et.tag = tg.tag
                                            )
                                      )
                                  )
                            ) THEN
                                RAISE EXCEPTION 'condition' USING ERRCODE = 'P0001', HINT = 'concurrency', DETAIL = coalesce(p_condition_after::text, '');
                            END IF;
                        END LOOP;
                    END IF;
                END IF;

                -- Baseline is the caller's expected version, or the version visible
                -- before this upsert. A second writer that saw the same baseline
                -- waits on the row, then fails here, before any sequence number is taken.
                IF p_expected IS NULL THEN
                    SELECT current_version INTO v_baseline
                    FROM {names.Streams}
                    WHERE tenant_id = p_tenant_id AND stream_id = p_stream_id;
                    IF NOT FOUND THEN
                        v_baseline := -1;
                    END IF;
                ELSE
                    v_baseline := p_expected;
                END IF;

                INSERT INTO {names.Streams} AS s (
                    tenant_id, stream_id, aggregate_type, aggregate_id,
                    current_version, last_global_position, created_at, last_event_at,
                    event_count, tags, status)
                VALUES (
                    p_tenant_id, p_stream_id, p_aggregate_type, p_aggregate_id,
                    -1, 0, p_recorded_at, p_recorded_at,
                    0, ARRAY[]::text[], 'Active')
                ON CONFLICT (tenant_id, stream_id) DO UPDATE
                    SET current_version = s.current_version
                    WHERE s.current_version = v_baseline
                RETURNING s.current_version, s.event_count
                INTO v_current, v_count;

                IF NOT FOUND THEN
                    SELECT current_version INTO v_actual
                    FROM {names.Streams}
                    WHERE tenant_id = p_tenant_id AND stream_id = p_stream_id;
                    IF NOT FOUND THEN
                        v_actual := -1;
                    END IF;
                    RAISE EXCEPTION 'concurrency'
                        USING ERRCODE = 'P0001', HINT = 'concurrency',
                              DETAIL = v_baseline::text || ':' || v_actual::text;
                END IF;

                IF v_count = 0 AND p_expected IS NOT NULL AND p_expected <> -1 THEN
                    RAISE EXCEPTION 'concurrency'
                        USING ERRCODE = 'P0001', HINT = 'concurrency',
                              DETAIL = p_expected::text || ':-1';
                END IF;

                v_n := coalesce(array_length(p_event_ids, 1), 0);
                IF v_n < 1 THEN
                    RAISE EXCEPTION 'empty append' USING ERRCODE = 'P0001', HINT = 'empty';
                END IF;

                IF p_is_dcb THEN
                    v_start := 0;
                ELSIF v_current < 0 THEN
                    v_start := 1;
                ELSE
                    v_start := v_current + 1;
                END IF;

                -- nextval in a loop so position order matches event order.
                -- A set-based SELECT nextval() does not promise that order.
                v_positions := ARRAY[]::bigint[];
                FOR v_i IN 1 .. v_n LOOP
                    v_positions := v_positions || nextval('{names.Sequence}'::regclass);
                END LOOP;

                BEGIN
                    INSERT INTO {names.Events} (
                        tenant_id, stream_id, version, global_position, event_id,
                        event_type_id, schema_version, codec_id, data, bdata, metadata,
                        tags, partition_hash, recorded_at)
                    SELECT
                        p_tenant_id,
                        p_stream_id,
                        v_start + u.ord - 1,
                        v_positions[u.ord],
                        u.event_id,
                        u.type_id,
                        u.schema_version,
                        u.codec_id,
                        u.data,
                        u.bdata,
                        u.metadata,
                        (
                            SELECT CASE WHEN count(*) = 0 THEN NULL ELSE array_agg(tv.tag) END
                            FROM unnest(p_tag_ords, p_tag_values) AS tv(ord, tag)
                            WHERE tv.ord = u.ord
                        ),
                        p_partition_hash,
                        p_recorded_at
                    FROM unnest(
                        p_event_ids, p_type_ids, p_schema_versions, p_codec_ids, p_data, p_bdata, p_metadata
                    ) WITH ORDINALITY AS u(event_id, type_id, schema_version, codec_id, data, bdata, metadata, ord);

                    INSERT INTO {names.Tags} (tag, global_position, tenant_id)
                    SELECT tv.tag, v_positions[tv.ord], p_tenant_id
                    FROM unnest(p_tag_ords, p_tag_values) AS tv(ord, tag)
                    WHERE tv.tag IS NOT NULL AND length(tv.tag) > 0;
                EXCEPTION
                    WHEN unique_violation THEN
                        GET STACKED DIAGNOSTICS v_constraint = CONSTRAINT_NAME;
                        IF v_constraint LIKE '%event_id%' OR v_constraint ILIKE '%event_id%' THEN
                            SELECT event_id::text INTO v_detail
                            FROM {names.Events}
                            WHERE tenant_id = p_tenant_id AND event_id = ANY(p_event_ids)
                            LIMIT 1;
                            RAISE EXCEPTION 'event already appended'
                                USING ERRCODE = 'P0001', HINT = 'event_already_appended', DETAIL = coalesce(v_detail, '');
                        ELSE
                            SELECT current_version INTO v_actual
                            FROM {names.Streams}
                            WHERE tenant_id = p_tenant_id AND stream_id = p_stream_id;
                            RAISE EXCEPTION 'concurrency'
                                USING ERRCODE = 'P0001', HINT = 'concurrency',
                                      DETAIL = (v_start - 1)::text || ':' || coalesce(v_actual, v_start)::text;
                        END IF;
                END;

                UPDATE {names.Streams} AS s
                SET current_version = v_start + v_n - 1,
                    last_global_position = v_positions[v_n],
                    last_event_at = p_recorded_at,
                    event_count = s.event_count + v_n,
                    tags = (
                        SELECT CASE WHEN count(*) = 0 THEN NULL ELSE array_agg(DISTINCT t) END
                        FROM unnest(coalesce(s.tags, ARRAY[]::text[]) || coalesce(p_stream_tags, ARRAY[]::text[])) AS t
                    )
                WHERE s.tenant_id = p_tenant_id AND s.stream_id = p_stream_id;

                IF p_notify THEN
                    PERFORM pg_notify(p_notify_channel, v_positions[v_n]::text);
                END IF;

                RETURN QUERY
                SELECT v_positions[g], (v_start + g - 1)::bigint
                FROM generate_series(1, v_n) AS g;
            END;
            $fn$;
            """;
    }
}
