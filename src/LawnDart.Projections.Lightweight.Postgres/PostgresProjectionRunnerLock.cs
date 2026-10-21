using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.Projections.Lightweight.Hosting;

namespace LawnDart.Projections.Lightweight.Postgres;

/// <summary>
/// One running copy per projection per node number.
/// </summary>
/// <remarks>
/// On a direct connection this holds a session lock. A second copy with the same
/// node number waits and logs a warning. If the holder's connection drops, or the
/// database restarts, the holder stops before its next save and the waiting copy
/// takes over. Each new leader stores a higher epoch on the checkpoint row. A
/// checkpoint save from a copy that started with an older epoch is rejected.
/// When the connection cannot hold a session lock, this uses a lease row instead
/// and logs that failover is slower. Set <see cref="PostgresProjectionStoreOptions.EnableSingleRunnerLock"/>
/// to false and do not register this type to let every copy run.
/// </remarks>
public sealed class PostgresProjectionRunnerLock : IProjectionRunnerLock
{
    private readonly PostgresCheckpointStore _checkpoints;
    private readonly PostgresProjectionStoreOptions _options;
    private readonly PostgresProjectionNames _names;
    private readonly ILogger? _logger;
    private readonly SemaphoreSlim _modeGate = new(1, 1);
    private bool _modeChosen;
    private bool _useLease;

    /// <summary>Creates a lock that writes its epoch through <paramref name="checkpoints"/>.</summary>
    public PostgresProjectionRunnerLock(
        PostgresCheckpointStore checkpoints,
        PostgresProjectionStoreOptions options,
        ILogger<PostgresProjectionRunnerLock>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(checkpoints);
        ArgumentNullException.ThrowIfNull(options);
        options.EnsureValid();
        _checkpoints = checkpoints;
        _options = options;
        _names = new PostgresProjectionNames(options.SchemaName);
        _logger = logger;
    }

    /// <inheritdoc/>
    public async Task<IProjectionRunnerLease> AcquireAsync(
        string projectionType,
        int nodeId,
        CancellationToken cancellationToken = default)
    {
        await EnsureModeAsync(cancellationToken).ConfigureAwait(false);
        return _useLease
            ? await AcquireLeaseAsync(projectionType, nodeId, cancellationToken).ConfigureAwait(false)
            : await AcquireSessionAsync(projectionType, nodeId, cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureModeAsync(CancellationToken cancellationToken)
    {
        if (_modeChosen)
            return;
        await _modeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_modeChosen)
                return;
            _useLease = _options.RunnerLockMode switch
            {
                PostgresRunnerLockMode.Lease => true,
                PostgresRunnerLockMode.SessionLock => false,
                _ => PostgresPoolerHost.LooksLikeTransactionPooler(_options.LockSource)
            };
            _modeChosen = true;
        }
        finally
        {
            _modeGate.Release();
        }
    }

    private async Task<IProjectionRunnerLease> AcquireSessionAsync(
        string projectionType,
        int nodeId,
        CancellationToken cancellationToken)
    {
        var warned = false;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PostgresProjectionConnections.ClearIdle(_options.ConnectionString!);
            var connection = new NpgsqlConnection(Dedicated(_options.LockSource));
            try
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                await ApplyKeepAliveAsync(connection, cancellationToken).ConfigureAwait(false);
                if (!await TryLockAsync(connection, projectionType, nodeId, cancellationToken).ConfigureAwait(false))
                {
                    if (!warned)
                    {
                        warned = true;
                        if (_logger is not null)
                            PostgresProjectionLog.WaitingForNode(_logger, projectionType, nodeId);
                    }

                    await LockAsync(connection, projectionType, nodeId, cancellationToken).ConfigureAwait(false);
                }

                var epoch = await RaiseEpochAsync(connection, projectionType, nodeId, cancellationToken).ConfigureAwait(false);
                _checkpoints.BindLeaderEpoch(projectionType, nodeId, epoch);
                var pid = await BackendPidAsync(connection, cancellationToken).ConfigureAwait(false);
                var lease = new PostgresSessionLease(
                    connection, _logger, _names.Schema, projectionType, nodeId, epoch, pid, _options.LockProbeInterval);
                lease.Start();
                return lease;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                if (cancellationToken.IsCancellationRequested)
                    throw;
                PostgresProjectionConnections.ClearIdle(_options.ConnectionString!);
                await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<IProjectionRunnerLease> AcquireLeaseAsync(
        string projectionType,
        int nodeId,
        CancellationToken cancellationToken)
    {
        if (_logger is not null)
            PostgresProjectionLog.LeaseMode(_logger, projectionType, nodeId, _options.LeaseDuration.TotalSeconds);

        var holder = Guid.NewGuid();
        var warned = false;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                PostgresProjectionConnections.ClearIdle(_options.ConnectionString!);
                var epoch = await TryTakeLeaseAndRaiseEpochAsync(projectionType, nodeId, holder, cancellationToken).ConfigureAwait(false);
                if (epoch is long taken)
                {
                    _checkpoints.BindLeaderEpoch(projectionType, nodeId, taken);
                    var lease = new PostgresPooledLease(
                        _options, _names, _logger, projectionType, nodeId, taken, holder);
                    lease.Start();
                    return lease;
                }

                if (!warned)
                {
                    warned = true;
                    if (_logger is not null)
                        PostgresProjectionLog.WaitingForNode(_logger, projectionType, nodeId);
                }

                var expires = await ReadExpiryAsync(projectionType, nodeId, cancellationToken).ConfigureAwait(false);
                var wait = expires is null ? TimeSpan.FromMilliseconds(50) : expires.Value - DateTime.UtcNow;
                if (wait < TimeSpan.FromMilliseconds(20))
                    wait = TimeSpan.FromMilliseconds(20);
                if (wait > _options.LeaseDuration)
                    wait = _options.LeaseDuration;
                await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (cancellationToken.IsCancellationRequested)
                    throw;
                PostgresProjectionConnections.ClearIdle(_options.ConnectionString!);
                await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<long?> TryTakeLeaseAndRaiseEpochAsync(
        string projectionType, int nodeId, Guid holder, CancellationToken cancellationToken)
    {
        var sql = $"""
            WITH taken AS (
                INSERT INTO {_names.Leases} AS l (projection_type, node_id, holder_id, expires_at)
                VALUES (@type, @node, @holder, now() + @lease)
                ON CONFLICT (projection_type, node_id) DO UPDATE
                SET holder_id = EXCLUDED.holder_id,
                    expires_at = EXCLUDED.expires_at
                WHERE l.expires_at <= now() OR l.holder_id = EXCLUDED.holder_id
                RETURNING 1
            ),
            raised AS (
                INSERT INTO {_names.Checkpoints} AS c
                    (projection_type, node_id, last_position, last_updated, total_events, leader_epoch)
                SELECT @type, @node, -1, now(), 0, 1
                FROM taken
                ON CONFLICT (projection_type, node_id) DO UPDATE
                SET leader_epoch = c.leader_epoch + 1
                RETURNING leader_epoch
            )
            SELECT leader_epoch FROM raised
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", projectionType);
        command.Parameters.AddWithValue("node", nodeId);
        command.Parameters.AddWithValue("holder", holder);
        command.Parameters.Add(new NpgsqlParameter("lease", NpgsqlDbType.Interval) { Value = _options.LeaseDuration });
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull ? null : Convert.ToInt64(result);
    }

    private async Task<long> RaiseEpochAsync(
        NpgsqlConnection connection, string projectionType, int nodeId, CancellationToken cancellationToken)
    {
        var sql = $"""
            INSERT INTO {_names.Checkpoints} AS c
                (projection_type, node_id, last_position, last_updated, total_events, leader_epoch)
            VALUES (@type, @node, -1, now(), 0, 1)
            ON CONFLICT (projection_type, node_id) DO UPDATE
            SET leader_epoch = c.leader_epoch + 1
            RETURNING leader_epoch
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", projectionType);
        command.Parameters.AddWithValue("node", nodeId);
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
    }

    private async Task ApplyKeepAliveAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = new NpgsqlCommand(
                """
                SELECT set_config('tcp_keepalives_idle', @idle, false),
                       set_config('tcp_keepalives_interval', @interval, false),
                       set_config('tcp_keepalives_count', @count, false)
                """,
                connection);
            command.Parameters.AddWithValue("idle", _options.LockTcpKeepAliveIdleSeconds.ToString());
            command.Parameters.AddWithValue("interval", _options.LockTcpKeepAliveIntervalSeconds.ToString());
            command.Parameters.AddWithValue("count", _options.LockTcpKeepAliveCount.ToString());
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (NpgsqlException)
        {
            // The session still has Npgsql's keepalive when the server rejects these settings.
        }
    }

    private async Task<DateTime?> ReadExpiryAsync(string projectionType, int nodeId, CancellationToken cancellationToken)
    {
        var sql = $"""
            SELECT expires_at FROM {_names.Leases}
            WHERE projection_type = @type AND node_id = @node
            """;
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", projectionType);
        command.Parameters.AddWithValue("node", nodeId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (result is null or DBNull)
            return null;
        return DateTime.SpecifyKind((DateTime)result, DateTimeKind.Utc);
    }

    private ValueTask<NpgsqlConnection> OpenAsync(CancellationToken cancellationToken)
        => PostgresProjectionConnections.Shared(_options.ConnectionString!).OpenConnectionAsync(cancellationToken);

    private string Dedicated(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = false,
            KeepAlive = _options.LockKeepAliveSeconds
        };
        if (string.IsNullOrEmpty(builder.ApplicationName))
            builder.ApplicationName = "lawndart-projection-lock";
        return builder.ConnectionString;
    }

    private Task<bool> TryLockAsync(NpgsqlConnection connection, string projectionType, int nodeId, CancellationToken cancellationToken)
        => AdvisoryAsync(connection, "pg_try_advisory_lock", projectionType, nodeId, cancellationToken);

    private async Task LockAsync(NpgsqlConnection connection, string projectionType, int nodeId, CancellationToken cancellationToken)
    {
        await using var command = AdvisoryCommand(connection, "pg_advisory_lock", projectionType, nodeId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> AdvisoryAsync(
        NpgsqlConnection connection, string function, string projectionType, int nodeId, CancellationToken cancellationToken)
    {
        await using var command = AdvisoryCommand(connection, function, projectionType, nodeId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is true;
    }

    private NpgsqlCommand AdvisoryCommand(NpgsqlConnection connection, string function, string projectionType, int nodeId)
    {
        var command = new NpgsqlCommand(
            $"SELECT {function}(hashtext(@schema), hashtext(@slot))", connection);
        command.Parameters.AddWithValue("schema", _names.Schema);
        command.Parameters.AddWithValue("slot", projectionType + ":" + nodeId.ToString());
        return command;
    }

    private static async Task<int> BackendPidAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_backend_pid()", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
    }

}

internal sealed class PostgresSessionLease : IProjectionRunnerLease
{
    private readonly NpgsqlConnection _connection;
    private readonly ILogger? _logger;
    private readonly string _schema;
    private readonly string _projectionType;
    private readonly int _nodeId;
    private readonly TimeSpan _interval;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _gate = new(1, 1);
    private int _held = 1;
    private int _disposed;
    private Task? _probe;

    internal PostgresSessionLease(
        NpgsqlConnection connection,
        ILogger? logger,
        string schema,
        string projectionType,
        int nodeId,
        long epoch,
        int sessionProcessId,
        TimeSpan interval)
    {
        _connection = connection;
        _logger = logger;
        _schema = schema;
        _projectionType = projectionType;
        _nodeId = nodeId;
        Epoch = epoch;
        SessionProcessId = sessionProcessId;
        _interval = interval;
    }

    public bool IsHeld => Volatile.Read(ref _held) == 1;

    public long Epoch { get; }

    public int? SessionProcessId { get; }

    internal void Start() => _probe = ProbeAsync(_stop.Token);

    /// <summary>Stops the probe and closes the session so the lock drops.</summary>
    internal void Abandon()
    {
        Volatile.Write(ref _held, 0);
        _stop.Cancel();
        try
        {
            _connection.Close();
        }
        catch (NpgsqlException)
        {
            // The session is already gone.
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;
        Volatile.Write(ref _held, 0);
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_probe is not null)
        {
            try
            {
                await _probe.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // The probe stopped.
            }
        }

        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_connection.State == System.Data.ConnectionState.Open)
            {
                await using var command = new NpgsqlCommand(
                    "SELECT pg_advisory_unlock(hashtext(@schema), hashtext(@slot))", _connection);
                command.Parameters.AddWithValue("schema", _schema);
                command.Parameters.AddWithValue("slot", _projectionType + ":" + _nodeId.ToString());
                try
                {
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
                catch (NpgsqlException)
                {
                    // The session is already gone, so the lock is gone with it.
                }
            }
        }
        finally
        {
            _gate.Release();
            await _connection.DisposeAsync().ConfigureAwait(false);
            _stop.Dispose();
        }
    }

    private async Task ProbeAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(_interval, cancellationToken).ConfigureAwait(false);
                await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    await using var command = new NpgsqlCommand("SELECT 1", _connection);
                    await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _gate.Release();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Dispose or abandon.
        }
        catch (Exception)
        {
            if (Volatile.Read(ref _held) == 1 && _logger is not null)
                PostgresProjectionLog.LockLost(_logger, _projectionType, _nodeId);
            Volatile.Write(ref _held, 0);
        }
    }
}

internal sealed class PostgresPooledLease : IProjectionRunnerLease
{
    private readonly PostgresProjectionStoreOptions _options;
    private readonly PostgresProjectionNames _names;
    private readonly ILogger? _logger;
    private readonly string _projectionType;
    private readonly int _nodeId;
    private readonly Guid _holder;
    private readonly CancellationTokenSource _stop = new();
    private int _held = 1;
    private int _release = 1;
    private int _disposed;
    private Task? _renew;

    internal PostgresPooledLease(
        PostgresProjectionStoreOptions options,
        PostgresProjectionNames names,
        ILogger? logger,
        string projectionType,
        int nodeId,
        long epoch,
        Guid holder)
    {
        _options = options;
        _names = names;
        _logger = logger;
        _projectionType = projectionType;
        _nodeId = nodeId;
        Epoch = epoch;
        _holder = holder;
    }

    public bool IsHeld => Volatile.Read(ref _held) == 1;

    public long Epoch { get; }

    internal void Start() => _renew = RenewAsync(_stop.Token);

    /// <summary>Stops renewal and leaves the lease row to expire.</summary>
    internal void Abandon()
    {
        Volatile.Write(ref _release, 0);
        Volatile.Write(ref _held, 0);
        _stop.Cancel();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;
        var release = Volatile.Read(ref _release) == 1;
        Volatile.Write(ref _held, 0);
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_renew is not null)
        {
            try
            {
                await _renew.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Renewal stopped.
            }
        }

        if (release)
            await DeleteAsync(CancellationToken.None).ConfigureAwait(false);
        _stop.Dispose();
    }

    private async Task RenewAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(_options.LeaseRenewInterval, cancellationToken).ConfigureAwait(false);
                if (!await RenewOnceAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (_logger is not null)
                        PostgresProjectionLog.LockLost(_logger, _projectionType, _nodeId);
                    Volatile.Write(ref _held, 0);
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Dispose or abandon.
        }
        catch (Exception)
        {
            if (_logger is not null)
                PostgresProjectionLog.LockLost(_logger, _projectionType, _nodeId);
            Volatile.Write(ref _held, 0);
        }
    }

    private async Task<bool> RenewOnceAsync(CancellationToken cancellationToken)
    {
        var sql = $"""
            UPDATE {_names.Leases}
            SET expires_at = now() + @lease
            WHERE projection_type = @type AND node_id = @node
              AND holder_id = @holder AND expires_at > now()
            """;
        await using var connection = await PostgresProjectionConnections.Shared(_options.ConnectionString!).OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("type", _projectionType);
        command.Parameters.AddWithValue("node", _nodeId);
        command.Parameters.AddWithValue("holder", _holder);
        command.Parameters.Add(new NpgsqlParameter("lease", NpgsqlDbType.Interval) { Value = _options.LeaseDuration });
        var rows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return rows == 1;
    }

    private async Task DeleteAsync(CancellationToken cancellationToken)
    {
        try
        {
            var sql = $"""
                DELETE FROM {_names.Leases}
                WHERE projection_type = @type AND node_id = @node AND holder_id = @holder
                """;
            await using var connection = await PostgresProjectionConnections.Shared(_options.ConnectionString!).OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue("type", _projectionType);
            command.Parameters.AddWithValue("node", _nodeId);
            command.Parameters.AddWithValue("holder", _holder);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (NpgsqlException)
        {
            // The row expires on its own when this connection cannot reach the database.
        }
    }
}
