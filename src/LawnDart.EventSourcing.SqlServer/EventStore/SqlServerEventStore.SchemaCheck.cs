using LawnDart;
using LawnDart.EventSourcing.SqlServer.Snapshots;
using LawnDart.Sql;
using Microsoft.Data.SqlClient;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

public partial class SqlServerEventStore
{
    /// <summary>
    /// Reads the events format stamp and the tables and trigger this version writes.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether the schema matches, which stamp was found, and any missing objects.</returns>
    /// <remarks>
    /// This method does not create or change objects. Call it at startup.
    /// <see cref="SchemaCheckResult.ThrowIfMismatch"/> throws when the result does not match.
    /// Stream tables, <c>StreamIdBin</c>, <c>UX_StreamIdBin</c>, and the stream tag trigger
    /// are required only when the stream registry is on.
    /// <c>UX_EventId</c> and <c>UX_SequencePosition</c> are required on the events table.
    /// </remarks>
    public async Task<SchemaCheckResult> VerifySchemaAsync(CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var disposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await SqlSchemaProbe.RequireViewDefinitionAsync(connection, _schemaName, cancellationToken).ConfigureAwait(false);

        var missing = new List<string>();
        string? found = null;
        if (await SqlSchemaProbe.ExistsAsync(connection, _schemaName, _tableName, "U", cancellationToken).ConfigureAwait(false))
            found = await SqlSchemaProbe.ReadStampAsync(connection, _schemaName, _tableName, SchemaFormatPropertyName, cancellationToken).ConfigureAwait(false);
        else
            missing.Add(_tableName);

        await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, _typesTableName, "U", missing, cancellationToken).ConfigureAwait(false);
        await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, _tagsTableName, "U", missing, cancellationToken).ConfigureAwait(false);
        await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, AppendFenceTableName, "U", missing, cancellationToken).ConfigureAwait(false);
        await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, "EventSequencePosition", "SO", missing, cancellationToken).ConfigureAwait(false);
        await SqlSchemaProbe.AddMissingIndexAsync(connection, _schemaName, _tableName, "UX_EventId", missing, cancellationToken).ConfigureAwait(false);
        await SqlSchemaProbe.AddMissingIndexAsync(connection, _schemaName, _tableName, "UX_SequencePosition", missing, cancellationToken).ConfigureAwait(false);

        if (_enableRegistry)
        {
            await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, _registryTableName, "U", missing, cancellationToken).ConfigureAwait(false);
            await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, _streamTagsTableName, "U", missing, cancellationToken).ConfigureAwait(false);
            await SqlSchemaProbe.AddMissingIndexAsync(connection, _schemaName, _registryTableName, "UX_StreamIdBin", missing, cancellationToken).ConfigureAwait(false);
            await SqlSchemaProbe.AddMissingColumnAsync(connection, _qRegistry, "StreamIdBin", missing, cancellationToken).ConfigureAwait(false);
            var trigger = "TR_" + _schemaName + "_" + _registryTableName + "_StreamTags";
            await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, trigger, "TR", missing, cancellationToken).ConfigureAwait(false);
        }

        var dcb = SqlServerSnapshotSchema.ResolveTableName(
            _options.DcbSnapshotsTableName, SqlServerSnapshotSchema.DefaultDcbTable, _tableName);
        var events = SqlServerSnapshotSchema.ResolveTableName(
            _options.EventSnapshotsTableName, SqlServerSnapshotSchema.DefaultEventTable, _tableName);
        await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, dcb, "U", missing, cancellationToken).ConfigureAwait(false);
        await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, events, "U", missing, cancellationToken).ConfigureAwait(false);

        return SqlSchemaProbe.Finish(CurrentSchemaFormat, found, missing);
    }
}
