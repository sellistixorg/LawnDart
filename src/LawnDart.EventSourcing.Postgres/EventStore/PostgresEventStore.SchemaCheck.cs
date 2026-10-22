using System.Globalization;
using LawnDart;
using LawnDart.EventSourcing.Postgres.Snapshots;

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
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await PostgresSchemaProbe.CheckAsync(
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
                new("sync_stream_tags", null, 0, null)
            ],
            [new("streams_sync_tags", _names.Streams)],
            cancellationToken).ConfigureAwait(false);
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
            new("ix_streams_stream_id", PgIdentifier.Qualify(_names.Schema, "ix_streams_stream_id")),
            new(UniqueKey(_options.EventsTableName, "event_id", "tenant_id"), Qualify(UniqueKey(_options.EventsTableName, "event_id", "tenant_id"))),
            new(UniqueKey(_options.EventsTableName, "global_position", "tenant_id"), Qualify(UniqueKey(_options.EventsTableName, "global_position", "tenant_id")))
        };

        if (!_options.Partitioning)
            return relations;

        relations.Add(new(_options.EventsTableName + "_shared", _names.EventsShared));
        relations.Add(new(_options.EventTagsTableName + "_shared", _names.TagsShared));
        relations.Add(new(_names.StreamTagsTable + "_shared", _names.StreamTagsShared));
        for (var i = 0; i < _options.PartitionCount; i++)
        {
            var suffix = "_shared_h" + i.ToString(CultureInfo.InvariantCulture);
            relations.Add(new(_options.EventsTableName + suffix, _names.EventsHash(i)));
            relations.Add(new(_options.EventTagsTableName + suffix, _names.TagsHash(i)));
            relations.Add(new(_names.StreamTagsTable + suffix, _names.StreamTagsHash(i)));
        }

        return relations;
    }

    private string Qualify(string name) => PgIdentifier.Qualify(_names.Schema, name);

    private static string UniqueKey(string table, string first, string second)
        => table + "_" + first + "_" + second + "_key";
}
