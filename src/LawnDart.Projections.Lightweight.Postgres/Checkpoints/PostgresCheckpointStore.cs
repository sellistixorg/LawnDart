using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
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

    /// <summary>Creates a checkpoint store. Call <see cref="InitializeSchemaAsync"/> before the first write.</summary>
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
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenMigrationAsync(cancellationToken).ConfigureAwait(false);
        await PostgresProjectionSchema.EnsureAsync(
            connection, _names, PostgresProjectionSchema.AppRole(_options), cancellationToken).ConfigureAwait(false);
        if (_logger is not null)
            PostgresProjectionLog.SchemaReady(_logger, _names.Schema);
    }

    /// <summary>
    /// The next <see cref="SaveViewsAndCheckpointAsync"/> writes the views and then throws
    /// before the checkpoint write. Both stay as they were. Used by tests.
    /// </summary>
    internal void FailNextFlushAfterViews() => Interlocked.Exchange(ref _failNextFlush, 1);

    /// <summary>
    /// Raises the epoch on the checkpoint row and returns the new number.
    /// A missing row is inserted at position -1 so the number has a place to live.
    /// </summary>
    internal async Task<long> RaiseEpochAsync(string projectionType, int nodeId, CancellationToken cancellationToken)
    {
        var sql = $"""
            INSERT INTO {_names.Checkpoints} AS c
                (projection_type, node_id, last_position, last_updated, total_events, leader_epoch)
            VALUES (@type, @node, -1, now(), 0, 1)
            ON CONFLICT (projection_type, node_id) DO UPDATE
            SET leader_epoch = c.leader_epoch + 1
            RETURNING leader_epoch
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", projectionType);
        command.Parameters.AddWithValue("node", nodeId);
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(value);
    }

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
        return ReadCheckpoint(reader);
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
        var fail = Interlocked.Exchange(ref _failNextFlush, 0) == 1;
        var sql = $"SELECT {_names.Flush}(@type, @node, @position, @updated, @total, @require_epoch, @epoch, @fail, @ids, @json, @view_checkpoints)";
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", projectionType);
        command.Parameters.AddWithValue("node", checkpoint.NodeId);
        command.Parameters.AddWithValue("position", checkpoint.LastSequencePosition);
        command.Parameters.Add(new NpgsqlParameter("updated", NpgsqlDbType.TimestampTz) { Value = DateTime.SpecifyKind(checkpoint.LastUpdated, DateTimeKind.Utc) });
        command.Parameters.AddWithValue("total", checkpoint.TotalEventsProcessed);
        command.Parameters.AddWithValue("require_epoch", requireEpoch);
        command.Parameters.AddWithValue("epoch", requireEpoch ? epoch : 0L);
        command.Parameters.AddWithValue("fail", fail);
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
        catch (PostgresException ex) when (ex.SqlState == "LD002")
        {
            throw new InvalidOperationException(ex.MessageText, ex);
        }
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

    /// <inheritdoc/>
    public async Task DeleteCheckpointAsync(
        string projectionType,
        int? nodeId = null,
        CancellationToken cancellationToken = default)
    {
        var sql = nodeId.HasValue
            ? $"DELETE FROM {_names.Checkpoints} WHERE projection_type = @type AND node_id = @node"
            : $"DELETE FROM {_names.Checkpoints} WHERE projection_type = @type";
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", projectionType);
        if (nodeId.HasValue)
            command.Parameters.AddWithValue("node", nodeId.Value);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<List<ProjectionCheckpoint>> ReadAllAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var list = new List<ProjectionCheckpoint>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            list.Add(ReadCheckpoint(reader));
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
