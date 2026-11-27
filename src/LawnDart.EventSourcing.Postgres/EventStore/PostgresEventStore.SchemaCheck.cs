using System.Globalization;
using LawnDart;
using LawnDart.EventSourcing.Postgres.Snapshots;
using Npgsql;

namespace LawnDart.EventSourcing.Postgres.EventStore;

public sealed partial class PostgresEventStore
{
    /// <summary>
    /// Reads the events format stamp and the tables, functions, and triggers this version writes.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether the schema matches, which stamp was found, and any missing objects.</returns>
    /// <remarks>
    /// This method does not create or change objects. It uses
    /// <see cref="PostgresEventStoreOptions.ConnectionString"/>, not
    /// <see cref="PostgresEventStoreOptions.MigrationConnectionString"/>.
    /// Call it at startup. <see cref="SchemaCheckResult.ThrowIfMismatch"/> throws when the result does not match.
    /// <c>append_to_stream</c> matches only when its whole body is the one this version writes
    /// and the marker version is exactly <see cref="PostgresAppendSql.BodyMarkerVersion"/>.
    /// </remarks>
    public async Task<SchemaCheckResult> VerifySchemaAsync(CancellationToken cancellationToken = default)
    {
        var expected = CurrentSchemaFormat.ToString(CultureInfo.InvariantCulture);
        var eventSnapshots = PgIdentifier.Qualify(_names.Schema, PostgresSnapshotSchema.EventTable(_options));
        var dcbSnapshots = PgIdentifier.Qualify(_names.Schema, PostgresSnapshotSchema.DcbTable(_options));
        var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var result = await PostgresSchemaProbe.CheckAsync(
            connection,
            expected,
            _names.Schema,
            _names.SchemaInfo,
            PostgresSchema.EventsComponent,
            Relations(eventSnapshots, dcbSnapshots),
            [new("exact_text", "exact_text")],
            [
                new(
                    "append_to_stream",
                    PostgresAppendSql.BodyMarkerPrefix,
                    PostgresAppendSql.BodyMarkerVersion,
                    PostgresAppendSql.ExpectedProsrc(_names, _options)),
                new(
                    PostgresNames.EnsurePartitionsFunction,
                    PostgresSchema.EnsureMarkerPrefix,
                    PostgresSchema.EnsureMarkerVersion,
                    PostgresSchema.ExpectedEnsureProsrc(_names, _options)),
                new("sync_stream_tags", null, 0, null)
            ],
            [new("streams_sync_tags", _names.Streams)],
            cancellationToken).ConfigureAwait(false);

        var ranges = await CheckRangesAsync(connection, cancellationToken).ConfigureAwait(false);
        if (ranges.Count == 0)
            return result;

        return new SchemaCheckResult(false, result.ExpectedFormat, result.FoundFormat, [.. result.Missing, .. ranges]);
    }

    /// <summary>
    /// Compares the stamped range setting with the options, and checks that the range
    /// holding the newest position exists. Ranges above it are created as appends need them.
    /// </summary>
    private async Task<List<string>> CheckRangesAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var problems = new List<string>();
        if (!await RelationExistsAsync(connection, _names.SchemaInfo, cancellationToken).ConfigureAwait(false))
            return problems;

        // partition_size came with format 7. Read it through the row, so an older
        // stamp table without the column gives the format mismatch and nothing else.
        var stamp = new NpgsqlCommand(
            $"SELECT s.format, s.partitioning, (to_jsonb(s) ->> 'partition_size')::bigint FROM {_names.SchemaInfo} AS s WHERE s.component = '{PostgresSchema.EventsComponent}'",
            connection);
        int? stampedFormat = null;
        bool? stampedRanges = null;
        long? stampedSize = null;
        await using (stamp.ConfigureAwait(false))
        {
            var reader = await stamp.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    stampedFormat = reader.GetInt32(0);
                    stampedRanges = reader.GetBoolean(1);
                    stampedSize = reader.IsDBNull(2) ? null : reader.GetInt64(2);
                }
            }
        }

        // Another format is already reported by the stamp check.
        if (stampedRanges is null || stampedFormat != CurrentSchemaFormat)
            return problems;

        var wanted = _options.UsesPositionPartitions;
        if (stampedRanges.Value != wanted || (wanted && stampedSize != _options.PartitionSize))
        {
            problems.Add(
                "position ranges " + (stampedRanges.Value ? "on, " + RangeSizeText(stampedSize) : "off")
                + " (options: " + (wanted ? "on, " + RangeSizeText(_options.PartitionSize) : "off") + ")");
            return problems;
        }

        if (!wanted || !await RelationExistsAsync(connection, _names.Sequence, cancellationToken).ConfigureAwait(false))
            return problems;

        // A read-only login may not read the sequence or the events. Use what it
        // can read, and skip this check when it can read neither.
        var head = new NpgsqlCommand(
            $"""
            SELECT
                CASE WHEN has_sequence_privilege(s.oid, 'SELECT,USAGE') THEN coalesce(pg_sequence_last_value(s.oid), 0) END,
                has_table_privilege(@events, 'SELECT')
            FROM (SELECT to_regclass(@sequence) AS oid) s
            """,
            connection);
        head.Parameters.AddWithValue("sequence", _names.Sequence);
        head.Parameters.AddWithValue("events", _names.Events);
        long? newestKnown = null;
        var canReadEvents = false;
        await using (head.ConfigureAwait(false))
        {
            var reader = await head.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    newestKnown = reader.IsDBNull(0) ? null : reader.GetInt64(0);
                    canReadEvents = reader.GetBoolean(1);
                }
            }
        }

        if (newestKnown is null && canReadEvents)
        {
            var stored = new NpgsqlCommand($"SELECT coalesce(max(global_position), 0) FROM {_names.Events}", connection);
            await using (stored.ConfigureAwait(false))
                newestKnown = Convert.ToInt64(await stored.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
        }

        if (newestKnown is not { } newest)
            return problems;

        var range = (Math.Max(newest, 1) - 1) / _options.PartitionSize;
        if (!await RangeAttachedAsync(connection, _names.EventsRange(range), _names.Events, cancellationToken).ConfigureAwait(false))
            problems.Add(_options.EventsTableName + "_p" + range.ToString(CultureInfo.InvariantCulture));
        if (!await RangeAttachedAsync(connection, _names.TagsRange(range), _names.Tags, cancellationToken).ConfigureAwait(false))
            problems.Add(_options.EventTagsTableName + "_p" + range.ToString(CultureInfo.InvariantCulture));
        return problems;
    }

    /// <summary>A range table counts only when it exists and is attached to its parent.</summary>
    private static async Task<bool> RangeAttachedAsync(NpgsqlConnection connection, string child, string parent, CancellationToken cancellationToken)
    {
        var command = new NpgsqlCommand(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_inherits i
                WHERE i.inhrelid = to_regclass(@child) AND i.inhparent = to_regclass(@parent))
            """,
            connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("child", child);
        command.Parameters.AddWithValue("parent", parent);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    private static string RangeSizeText(long? size)
        => size is { } value ? value.ToString("N0", CultureInfo.InvariantCulture) + " positions each" : "no size";

    private static async Task<bool> RelationExistsAsync(NpgsqlConnection connection, string qualified, CancellationToken cancellationToken)
    {
        var command = new NpgsqlCommand("SELECT to_regclass(@name) IS NOT NULL", connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("name", qualified);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    private List<PostgresSchemaProbe.Relation> Relations(string eventSnapshots, string dcbSnapshots)
    {
        var relations = new List<PostgresSchemaProbe.Relation>
        {
            new(_options.SchemaInfoTableName, _names.SchemaInfo),
            new(_options.EventTypesTableName, _names.Types),
            new(_options.EventsTableName, _names.Events),
            new(_options.StreamsTableName, _names.Streams),
            new(_names.StreamTagsTable, _names.StreamTags),
            new(_options.EventTagsTableName, _names.Tags),
            new("event_position", _names.Sequence),
            new(PostgresSnapshotSchema.EventTable(_options), eventSnapshots),
            new(PostgresSnapshotSchema.DcbTable(_options), dcbSnapshots),
            new(_options.OutboxTableName, _names.Outbox),
            new("ix_streams_stream_id", Qualify("ix_streams_stream_id")),
            new("ix_events_stream", Qualify("ix_events_stream")),
            new("ix_events_type", Qualify("ix_events_type")),
            new("ix_events_recorded_at", Qualify("ix_events_recorded_at"))
        };

        if (_options.Tenancy == Tenancy.Shared)
            relations.Add(new("ix_events_tenant", Qualify("ix_events_tenant")));

        return relations;
    }

    private string Qualify(string name) => PgIdentifier.Qualify(_names.Schema, name);
}
