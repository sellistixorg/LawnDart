using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using LawnDart;
using LawnDart.Projections;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Postgres;

/// <summary>
/// Postgres checkpoint store. A save keeps the stored position when the new
/// position is older. When this copy took the lead, the save also has to carry
/// the epoch it started with.
/// </summary>
public sealed class PostgresCheckpointStore : ICheckpointStore, IAtomicProjectionFlush
{
    private readonly PostgresProjectionStoreOptions _options;
    private readonly PostgresProjectionNames _names;
    private readonly IViewStore _views;
    private readonly ILogger<PostgresCheckpointStore>? _logger;
    private readonly ConcurrentDictionary<(string Type, int Node), long> _epochs = new();
    private int _failNextFlush;

    /// <summary>
    /// Runs after the checkpoint reset and before the view delete, inside the
    /// same transaction. Tests use this to save from a waiting copy in that gap.
    /// </summary>
    internal Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? BeforeDeletingViews { get; set; }

    /// <summary>Creates a checkpoint store. Call <see cref="InitializeSchemaAsync"/> from a deploy or admin step before the first write.</summary>
    public PostgresCheckpointStore(
        PostgresProjectionStoreOptions options,
        IViewStore views,
        ILogger<PostgresCheckpointStore>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(views);
        options.EnsureValid();
        _options = options;
        _names = new PostgresProjectionNames(options.SchemaName);
        _views = views;
        _logger = logger;
    }

    /// <summary>Schema-qualified checkpoint table.</summary>
    public string QualifiedTableName => _names.Checkpoints;

    /// <summary>
    /// Creates the checkpoint table, the lease table, and the flush function when they are missing.
    /// A later call replaces the function and does not drop rows.
    /// </summary>
    /// <remarks>
    /// Run this from a deploy job, a migration step, or an admin tool, once per deploy.
    /// Normal app startup should not call it. An older copy that restarts and runs it
    /// can change a schema a newer version already upgraded, including <c>save_projection_flush</c>.
    /// </remarks>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenMigrationAsync(cancellationToken).ConfigureAwait(false);
        await PostgresProjectionSchema.EnsureAsync(
            connection, _names, PostgresProjectionSchema.AppRole(_options), cancellationToken).ConfigureAwait(false);
        if (_logger is not null)
            PostgresProjectionLog.SchemaReady(_logger, _names.Schema);
    }

    /// <summary>
    /// Reads the projection tables and <c>save_projection_flush</c>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A match when the tables exist and the flush function body contains this version's marker.
    /// This store has no format stamp.
    /// </returns>
    /// <remarks>
    /// This method does not create or change objects. It uses
    /// <see cref="PostgresProjectionStoreOptions.ConnectionString"/>, not
    /// <see cref="PostgresProjectionStoreOptions.MigrationConnectionString"/>.
    /// Call it at startup. <see cref="SchemaCheckResult.ThrowIfMismatch"/> throws when the result does not match.
    /// </remarks>
    public async Task<SchemaCheckResult> VerifySchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        return await PostgresProjectionSchema.VerifyAsync(connection, _names, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The next <see cref="SaveViewsAndCheckpointAsync"/> writes the views and then throws
    /// before the checkpoint write. Both stay as they were. Used by tests.
    /// </summary>
    internal void FailNextFlushAfterViews() => Interlocked.Exchange(ref _failNextFlush, 1);

    /// <summary>Remembers the epoch this copy must send on later checkpoint saves.</summary>
    internal void BindLeaderEpoch(string projectionType, int nodeId, long epoch)
        => _epochs[(projectionType, nodeId)] = epoch;

    /// <inheritdoc/>
    public async Task<ProjectionCheckpoint?> GetCheckpointAsync(
        string projectionType,
        int nodeId,
        CancellationToken cancellationToken = default)
    {
        var sql = $"""
            SELECT projection_type, node_id, last_position, last_updated, total_events
            FROM {_names.Checkpoints}
            WHERE projection_type = @type AND node_id = @node
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", projectionType);
        command.Parameters.AddWithValue("node", nodeId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;
        var row = ReadCheckpoint(reader);
        return row.LastSequencePosition < 0 ? null : row;
    }

    /// <inheritdoc/>
    public Task SaveCheckpointAsync(ProjectionCheckpoint checkpoint, CancellationToken cancellationToken = default)
        => SaveViewsAndCheckpointAsync(
            checkpoint.ProjectionType,
            [],
            checkpoint,
            cancellationToken);

    /// <inheritdoc/>
    public async Task SaveViewsAndCheckpointAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        ProjectionCheckpoint checkpoint,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(views);
        ArgumentNullException.ThrowIfNull(checkpoint);
        var ids = Array.Empty<string>();
        var json = Array.Empty<string>();
        var viewCheckpoints = Array.Empty<long>();
        if (views.Count > 0)
            (ids, json, viewCheckpoints) = PostgresViewStore.Dedupe(views);
        var requireEpoch = _epochs.TryGetValue((projectionType, checkpoint.NodeId), out var epoch);
        if (Interlocked.Exchange(ref _failNextFlush, 0) == 1)
        {
            await using var failing = await OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await failing.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            if (ids.Length > 0)
                await WriteViewsAsync(failing, transaction, projectionType, ids, json, viewCheckpoints, checkpoint.LastUpdated, cancellationToken).ConfigureAwait(false);
            throw new InvalidOperationException(
                "The flush stopped after the view writes. Views and the checkpoint are unchanged.");
        }

        var sql = $"SELECT {_names.Flush}(@type, @node, @position, @updated, @total, @require_epoch, @epoch, @ids, @json, @view_checkpoints)";
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", projectionType);
        command.Parameters.AddWithValue("node", checkpoint.NodeId);
        command.Parameters.AddWithValue("position", checkpoint.LastSequencePosition);
        command.Parameters.Add(new NpgsqlParameter("updated", NpgsqlDbType.TimestampTz) { Value = DateTime.SpecifyKind(checkpoint.LastUpdated, DateTimeKind.Utc) });
        command.Parameters.AddWithValue("total", checkpoint.TotalEventsProcessed);
        command.Parameters.AddWithValue("require_epoch", requireEpoch);
        command.Parameters.AddWithValue("epoch", requireEpoch ? epoch : 0L);
        command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = ids });
        command.Parameters.Add(new NpgsqlParameter("json", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = json });
        command.Parameters.Add(new NpgsqlParameter("view_checkpoints", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = viewCheckpoints });
        try
        {
            await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState == "LD001")
        {
            throw new ProjectionLeadershipLostException(ex.MessageText, ex);
        }
    }

    private async Task WriteViewsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string projectionType,
        string[] ids,
        string[] json,
        long[] viewCheckpoints,
        DateTime updated,
        CancellationToken cancellationToken)
    {
        var sql = $"""
            INSERT INTO {_names.Views} (projection_type, instance_id, view_data, checkpoint, last_updated)
            SELECT @projection_type, u.instance_id, u.view_data::jsonb, u.checkpoint, @updated
            FROM unnest(@instance_ids, @view_data, @checkpoints) AS u(instance_id, view_data, checkpoint)
            ON CONFLICT (projection_type, instance_id) DO UPDATE
            SET view_data = EXCLUDED.view_data,
                checkpoint = EXCLUDED.checkpoint,
                last_updated = EXCLUDED.last_updated
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("projection_type", projectionType);
        command.Parameters.Add(new NpgsqlParameter("instance_ids", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = ids });
        command.Parameters.Add(new NpgsqlParameter("view_data", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = json });
        command.Parameters.Add(new NpgsqlParameter("checkpoints", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = viewCheckpoints });
        command.Parameters.Add(new NpgsqlParameter("updated", NpgsqlDbType.TimestampTz) { Value = DateTime.SpecifyKind(updated, DateTimeKind.Utc) });
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<IEnumerable<ProjectionCheckpoint>> GetCheckpointsForProjectionAsync(
        string projectionType,
        CancellationToken cancellationToken = default)
    {
        var sql = $"""
            SELECT projection_type, node_id, last_position, last_updated, total_events
            FROM {_names.Checkpoints}
            WHERE projection_type = @type
            ORDER BY node_id
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", projectionType);
        return await ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<IEnumerable<ProjectionCheckpoint>> GetAllCheckpointsAsync(
        CancellationToken cancellationToken = default)
    {
        var sql = $"""
            SELECT projection_type, node_id, last_position, last_updated, total_events
            FROM {_names.Checkpoints}
            ORDER BY projection_type, node_id
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        return await ReadAllAsync(command, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<long?> GetStreamCheckpointAsync(
        string projectionType,
        string streamId,
        CancellationToken cancellationToken = default)
    {
        var result = await _views.GetViewWithCheckpointAsync(projectionType, streamId, cancellationToken).ConfigureAwait(false);
        return result?.Checkpoint;
    }

    /// <inheritdoc/>
    public Task SaveStreamCheckpointAsync(
        string projectionType,
        string streamId,
        long sequencePosition,
        CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    /// <summary>
    /// Sets every checkpoint for <paramref name="projectionType"/> below zero, raises its epoch,
    /// and deletes its views. Both happen in one transaction. The checkpoint is cleared first.
    /// </summary>
    internal async Task ClearCheckpointAndViewsAsync(
        string projectionType,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await ResetCheckpointsAsync(connection, transaction, projectionType, nodeId: null, cancellationToken).ConfigureAwait(false);
        if (BeforeDeletingViews is { } beforeViews)
            await beforeViews(connection, transaction, cancellationToken).ConfigureAwait(false);
        var deleteViews = $"DELETE FROM {_names.Views} WHERE projection_type = @type";
        await using (var command = new NpgsqlCommand(deleteViews, connection, transaction))
        {
            command.Parameters.AddWithValue("type", projectionType);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task DeleteCheckpointAsync(
        string projectionType,
        int? nodeId = null,
        CancellationToken cancellationToken = default)
        => ResetCheckpointsAsync(connection: null, transaction: null, projectionType, nodeId, cancellationToken);

    private async Task ResetCheckpointsAsync(
        NpgsqlConnection? connection,
        NpgsqlTransaction? transaction,
        string projectionType,
        int? nodeId,
        CancellationToken cancellationToken)
    {
        var ownsConnection = connection is null;
        if (connection is null)
            connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Keep the row so the epoch stays. A later save that still has the old epoch is rejected.
            var sql = nodeId.HasValue
                ? $"""
                  UPDATE {_names.Checkpoints}
                  SET last_position = -1, total_events = 0, last_updated = now(), leader_epoch = leader_epoch + 1
                  WHERE projection_type = @type AND node_id = @node
                  """
                : $"""
                  UPDATE {_names.Checkpoints}
                  SET last_position = -1, total_events = 0, last_updated = now(), leader_epoch = leader_epoch + 1
                  WHERE projection_type = @type
                  """;
            await using var command = transaction is null
                ? new NpgsqlCommand(sql, connection)
                : new NpgsqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("type", projectionType);
            if (nodeId.HasValue)
                command.Parameters.AddWithValue("node", nodeId.Value);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (ownsConnection)
                await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task<List<ProjectionCheckpoint>> ReadAllAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var list = new List<ProjectionCheckpoint>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = ReadCheckpoint(reader);
            if (row.LastSequencePosition >= 0)
                list.Add(row);
        }
        return list;
    }

    private static ProjectionCheckpoint ReadCheckpoint(NpgsqlDataReader reader)
        => new()
        {
            ProjectionType = reader.GetString(0),
            NodeId = reader.GetInt32(1),
            LastSequencePosition = reader.GetInt64(2),
            LastUpdated = DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc),
            TotalEventsProcessed = reader.GetInt64(4)
        };

    private ValueTask<NpgsqlConnection> OpenAsync(CancellationToken cancellationToken)
        => PostgresProjectionConnections.Shared(_options.ConnectionString!).OpenConnectionAsync(cancellationToken);

    private ValueTask<NpgsqlConnection> OpenMigrationAsync(CancellationToken cancellationToken)
        => PostgresProjectionConnections.Shared(_options.MigrationSource).OpenConnectionAsync(cancellationToken);
}
