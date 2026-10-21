using Npgsql;

namespace LawnDart.Projections.Lightweight.Postgres;

internal static class PostgresProjectionSchema
{
    internal static async Task EnsureAsync(
        NpgsqlConnection connection,
        PostgresProjectionNames names,
        string? appRole,
        CancellationToken cancellationToken)
    {
        var sql = $"""
            CREATE SCHEMA IF NOT EXISTS {PgIdentifier.Quote(names.Schema)};

            CREATE TABLE IF NOT EXISTS {names.Views} (
                projection_type text NOT NULL,
                instance_id text NOT NULL,
                view_data jsonb NOT NULL,
                checkpoint bigint NOT NULL,
                last_updated timestamptz NOT NULL,
                PRIMARY KEY (projection_type, instance_id)
            );

            CREATE INDEX IF NOT EXISTS projection_views_type_updated
                ON {names.Views} (projection_type, last_updated DESC);

            CREATE TABLE IF NOT EXISTS {names.Checkpoints} (
                projection_type text NOT NULL,
                node_id integer NOT NULL,
                last_position bigint NOT NULL,
                last_updated timestamptz NOT NULL,
                total_events bigint NOT NULL,
                leader_epoch bigint NOT NULL DEFAULT 0,
                PRIMARY KEY (projection_type, node_id)
            );

            CREATE TABLE IF NOT EXISTS {names.Leases} (
                projection_type text NOT NULL,
                node_id integer NOT NULL,
                holder_id uuid NOT NULL,
                expires_at timestamptz NOT NULL,
                PRIMARY KEY (projection_type, node_id)
            );

            DROP FUNCTION IF EXISTS {names.Flush}(
                text, integer, bigint, timestamptz, bigint, boolean, bigint, boolean, text[], text[], bigint[]);

            CREATE OR REPLACE FUNCTION {names.Flush}(
                p_projection_type text,
                p_node_id integer,
                p_last_position bigint,
                p_last_updated timestamptz,
                p_total_events bigint,
                p_require_epoch boolean,
                p_epoch bigint,
                p_instance_ids text[],
                p_view_data text[],
                p_checkpoints bigint[]
            ) RETURNS integer
            LANGUAGE plpgsql
            AS $func$
            DECLARE
                current_epoch bigint;
            BEGIN
                PERFORM set_config('search_path', '{names.SearchPath}', true);

                IF p_require_epoch THEN
                    SELECT leader_epoch INTO current_epoch
                    FROM projection_checkpoints
                    WHERE projection_type = p_projection_type AND node_id = p_node_id
                    FOR UPDATE;

                    IF NOT FOUND OR current_epoch IS DISTINCT FROM p_epoch THEN
                        RAISE EXCEPTION 'The checkpoint for % on node % was not saved. This copy started with epoch % and the stored epoch is %. Another copy has taken the lead.',
                            p_projection_type, p_node_id, p_epoch, current_epoch
                            USING ERRCODE = 'LD001';
                    END IF;
                END IF;

                INSERT INTO projection_views (projection_type, instance_id, view_data, checkpoint, last_updated)
                SELECT p_projection_type, u.instance_id, u.view_data::jsonb, u.checkpoint, p_last_updated
                FROM unnest(p_instance_ids, p_view_data, p_checkpoints) AS u(instance_id, view_data, checkpoint)
                ON CONFLICT (projection_type, instance_id) DO UPDATE
                SET view_data = EXCLUDED.view_data,
                    checkpoint = EXCLUDED.checkpoint,
                    last_updated = EXCLUDED.last_updated;

                IF p_require_epoch THEN
                    UPDATE projection_checkpoints
                    SET last_position = p_last_position,
                        last_updated = p_last_updated,
                        total_events = p_total_events
                    WHERE projection_type = p_projection_type
                      AND node_id = p_node_id
                      AND leader_epoch = p_epoch
                      AND last_position <= p_last_position;
                ELSE
                    INSERT INTO projection_checkpoints AS c (
                        projection_type, node_id, last_position, last_updated, total_events, leader_epoch)
                    VALUES (
                        p_projection_type, p_node_id, p_last_position, p_last_updated, p_total_events, 0)
                    ON CONFLICT (projection_type, node_id) DO UPDATE
                    SET last_position = EXCLUDED.last_position,
                        last_updated = EXCLUDED.last_updated,
                        total_events = EXCLUDED.total_events
                    WHERE c.last_position <= EXCLUDED.last_position;
                END IF;

                RETURN 1;
            END;
            $func$;
            """;

        await using (var command = new NpgsqlCommand(sql, connection))
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        if (string.IsNullOrWhiteSpace(appRole))
            return;

        var grant = $"""
            GRANT USAGE ON SCHEMA {PgIdentifier.Quote(names.Schema)} TO {PgIdentifier.Quote(appRole)};
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {PgIdentifier.Quote(names.Schema)} TO {PgIdentifier.Quote(appRole)};
            GRANT EXECUTE ON ALL ROUTINES IN SCHEMA {PgIdentifier.Quote(names.Schema)} TO {PgIdentifier.Quote(appRole)};
            """;
        await using var grantCommand = new NpgsqlCommand(grant, connection);
        await grantCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    internal static string? AppRole(PostgresProjectionStoreOptions options)
    {
        var app = new NpgsqlConnectionStringBuilder(options.ConnectionString).Username;
        var owner = new NpgsqlConnectionStringBuilder(options.MigrationSource).Username;
        if (string.IsNullOrWhiteSpace(app) || string.Equals(app, owner, StringComparison.Ordinal))
            return null;
        return app;
    }
}
