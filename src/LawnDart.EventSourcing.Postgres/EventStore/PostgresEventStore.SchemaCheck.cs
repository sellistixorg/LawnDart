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
    /// <c>append_to_stream</c> matches only when its body contains the marker this version's setup writes.
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
            [
                new(_options.SchemaInfoTableName, _names.SchemaInfo),
                new(_options.EventTypesTableName, _names.Types),
                new(_options.EventsTableName, _names.Events),
                new(_options.StreamsTableName, _names.Streams),
                new(_names.StreamTagsTable, _names.StreamTags),
                new(_options.EventTagsTableName, _names.Tags),
                new("event_position", _names.Sequence),
                new(PostgresSnapshotSchema.EventTable(_options), eventSnapshots),
                new(PostgresSnapshotSchema.DcbTable(_options), dcbSnapshots),
                new(_options.OutboxTableName, _names.Outbox)
            ],
            [new("exact_text", "exact_text")],
            [
                new("append_to_stream", PostgresAppendSql.BodyMarker),
                new("sync_stream_tags", null)
            ],
            [new("streams_sync_tags", _names.Streams)],
            cancellationToken).ConfigureAwait(false);
    }
}
