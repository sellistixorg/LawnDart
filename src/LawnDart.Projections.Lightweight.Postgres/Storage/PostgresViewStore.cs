using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using LawnDart;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Postgres;

/// <summary>
/// Postgres view store. Each view is one jsonb row.
/// </summary>
public sealed class PostgresViewStore : IViewStore
{
    private readonly PostgresProjectionStoreOptions _options;
    private readonly PostgresProjectionNames _names;
    private readonly ILogger<PostgresViewStore>? _logger;

    /// <summary>Creates a view store. Call <see cref="InitializeSchemaAsync"/> from a deploy or admin step before the first write.</summary>
    public PostgresViewStore(
        PostgresProjectionStoreOptions options,
        ILogger<PostgresViewStore>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.EnsureValid();
        _options = options;
        _names = new PostgresProjectionNames(options.SchemaName);
        _logger = logger;
    }

    /// <summary>Schema-qualified view table.</summary>
    public string QualifiedTableName => _names.Views;

    /// <summary>
    /// Creates the schema and <c>projection_views</c> when they are missing.
    /// A later call does not create them again.
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

    /// <inheritdoc/>
    public Task SaveViewAsync(
        string projectionType,
        string instanceId,
        string viewData,
        long checkpoint,
        CancellationToken cancellationToken = default)
        => SaveViewsAsync(projectionType, [(instanceId, viewData, checkpoint)], cancellationToken);

    /// <inheritdoc/>
    public async Task SaveViewsAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(views);
        if (views.Count == 0)
            return;

        var (ids, json, checkpoints) = Dedupe(views);
        var sql = $"""
            INSERT INTO {_names.Views} (projection_type, instance_id, view_data, checkpoint, last_updated)
            SELECT @projection_type, u.instance_id, u.view_data::jsonb, u.checkpoint, @updated
            FROM unnest(@instance_ids, @view_data, @checkpoints) AS u(instance_id, view_data, checkpoint)
            ON CONFLICT (projection_type, instance_id) DO UPDATE
            SET view_data = EXCLUDED.view_data,
                checkpoint = EXCLUDED.checkpoint,
                last_updated = EXCLUDED.last_updated
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("projection_type", projectionType);
        command.Parameters.Add(new NpgsqlParameter("instance_ids", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = ids });
        command.Parameters.Add(new NpgsqlParameter("view_data", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = json });
        command.Parameters.Add(new NpgsqlParameter("checkpoints", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = checkpoints });
        command.Parameters.AddWithValue("updated", DateTime.UtcNow);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<string?> GetViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        var sql = $"""
            SELECT view_data::text
            FROM {_names.Views}
            WHERE projection_type = @projection_type AND instance_id = @instance_id
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("projection_type", projectionType);
        command.Parameters.AddWithValue("instance_id", instanceId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull ? null : (string)result;
    }

    /// <inheritdoc/>
    public async Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(
        string projectionType,
        CancellationToken cancellationToken = default)
    {
        var sql = $"""
            SELECT instance_id, view_data::text
            FROM {_names.Views}
            WHERE projection_type = @projection_type
            ORDER BY last_updated DESC, instance_id
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("projection_type", projectionType);
        var results = new List<(string, string)>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            results.Add((reader.GetString(0), reader.GetString(1)));
        return results;
    }

    /// <inheritdoc/>
    public async Task DeleteViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        var sql = $"""
            DELETE FROM {_names.Views}
            WHERE projection_type = @projection_type AND instance_id = @instance_id
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("projection_type", projectionType);
        command.Parameters.AddWithValue("instance_id", instanceId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        var sql = $"""
            SELECT view_data::text, checkpoint
            FROM {_names.Views}
            WHERE projection_type = @projection_type AND instance_id = @instance_id
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("projection_type", projectionType);
        command.Parameters.AddWithValue("instance_id", instanceId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;
        return (reader.GetString(0), reader.GetInt64(1));
    }

    /// <inheritdoc/>
    public async Task DeleteAllViewsAsync(string projectionType, CancellationToken cancellationToken = default)
    {
        var sql = $"DELETE FROM {_names.Views} WHERE projection_type = @projection_type";
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("projection_type", projectionType);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    internal static (string[] Ids, string[] Json, long[] Checkpoints) Dedupe(
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views)
    {
        var map = new Dictionary<string, (string ViewData, long Checkpoint)>(views.Count, StringComparer.Ordinal);
        foreach (var (instanceId, viewData, checkpoint) in views)
            map[instanceId] = (viewData, checkpoint);
        var ids = new string[map.Count];
        var json = new string[map.Count];
        var checkpoints = new long[map.Count];
        var index = 0;
        foreach (var (id, value) in map)
        {
            ids[index] = id;
            json[index] = value.ViewData;
            checkpoints[index] = value.Checkpoint;
            index++;
        }

        return (ids, json, checkpoints);
    }

    private ValueTask<NpgsqlConnection> OpenAsync(CancellationToken cancellationToken)
        => PostgresProjectionConnections.Shared(_options.ConnectionString!).OpenConnectionAsync(cancellationToken);

    private ValueTask<NpgsqlConnection> OpenMigrationAsync(CancellationToken cancellationToken)
        => PostgresProjectionConnections.Shared(_options.MigrationSource).OpenConnectionAsync(cancellationToken);
}
