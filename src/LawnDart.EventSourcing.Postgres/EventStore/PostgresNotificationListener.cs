using System.Globalization;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>How this store learns that new events were committed.</summary>
internal enum PostgresListenMode
{
    /// <summary>No subscriber has asked for a listener yet.</summary>
    Idle = 0,

    /// <summary>The startup probe is in progress.</summary>
    Probing = 1,

    /// <summary>A listen connection is receiving wake-ups.</summary>
    Listening = 2,

    /// <summary>The listen connection dropped. The listener will connect again.</summary>
    Reconnecting = 3,

    /// <summary>Subscriptions read on the poll interval. No listen connection is open.</summary>
    PollOnly = 4
}

/// <summary>
/// One listen connection per store. The wake-up payload is a position hint.
/// The events table remains the record. This connection never begins a transaction,
/// so it does not sit idle in a transaction and it does not hold the notification queue.
/// </summary>
internal sealed class PostgresNotificationListener
{
    internal const string PoolerReason =
        "The listen connection host looks like a transaction pooler, which cannot keep a listen session. Set ListenConnectionString to a direct Postgres connection, or to a session pooler.";

    internal const string ProbeReason =
        "A probe wake-up did not come back on the listen connection. Set ListenConnectionString to a direct Postgres connection, or to a session pooler.";

    private readonly PostgresEventStore _store;
    private readonly PostgresEventStoreOptions _options;
    private readonly string _channel;
    private readonly NpgsqlDataSource _dataSource;
    private readonly ILogger? _logger;
    private readonly object _gate = new();

    private CancellationTokenSource? _cts;
    private TaskCompletionSource _ready = CreateReady();
    private Task? _loop;
    private int _users;
    private int _mode;
    private int _warned;
    private int _loggedDrop;
    private int _backendPid;
    private int _waiting;
    private int _held;
    private int _delayingStart;
    private long _wakeCount;
    private long _notificationCount;
    private long _highest;
    private string? _probePayload;
    private TaskCompletionSource? _probeSeen;
    private bool _listeningEstablished;

    internal PostgresNotificationListener(
        PostgresEventStore store,
        PostgresEventStoreOptions options,
        string channel,
        NpgsqlDataSource dataSource,
        ILogger? logger)
    {
        _store = store;
        _options = options;
        _channel = channel;
        _dataSource = dataSource;
        _logger = logger;
    }

    internal PostgresListenMode Mode => (PostgresListenMode)Volatile.Read(ref _mode);

    internal Task Ready
    {
        get
        {
            lock (_gate)
                return _ready.Task;
        }
    }

    internal int BackendPid => Volatile.Read(ref _backendPid);

    internal long WakeCount => Interlocked.Read(ref _wakeCount);

    internal long NotificationCount => Interlocked.Read(ref _notificationCount);

    internal bool IsWaitingForNotification => Volatile.Read(ref _waiting) == 1;

    internal bool IsHeldBeforeWait => Volatile.Read(ref _held) == 1;

    internal bool IsDelayingStart => Volatile.Read(ref _delayingStart) == 1;

    /// <summary>Test seam. The listener waits here before it opens a connection.</summary>
    internal Task? DelayStart { get; set; }

    /// <summary>Test seam. After a dropped connection, the listener waits here before it connects again.</summary>
    internal Task? DelayReconnect { get; set; }

    /// <summary>Test seam. Replaces the growing pause between reconnect attempts.</summary>
    internal TimeSpan? ReconnectDelayOverride { get; set; }

    /// <summary>How long the startup probe waits for its own wake-up. Default 2 seconds.</summary>
    internal int ProbeTimeoutMilliseconds { get; set; } = 2000;

    /// <summary>Test seam. The listener waits here after LISTEN and before it reads wake-ups.</summary>
    internal Task? HoldBeforeWait { get; set; }

    /// <summary>
    /// Seconds of silence before the listen connection checks that the server is still there.
    /// Default 10. A restarted server is noticed on that check.
    /// </summary>
    internal int KeepAliveSeconds { get; set; } = 10;

    internal void AddUser()
    {
        lock (_gate)
        {
            _users++;
            if (_users == 1)
                StartLocked();
        }
    }

    internal void RemoveUser()
    {
        lock (_gate)
        {
            if (_users == 0)
                return;
            _users--;
            if (_users == 0)
                StopLocked();
        }
    }

    internal static TimeSpan BackoffDelay(int attempt)
    {
        var shift = Math.Clamp(attempt, 0, 5);
        var milliseconds = 200 * (1 << shift);
        if (milliseconds > 5000)
            milliseconds = 5000;
        return TimeSpan.FromMilliseconds(milliseconds);
    }

    private void StartLocked()
    {
        var cts = new CancellationTokenSource();
        _cts = cts;
        _ready = CreateReady();
        _listeningEstablished = false;
        _loop = Task.Run(() => RunAsync(cts));
    }

    private void StopLocked()
    {
        try
        {
            _cts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        _ready.TrySetCanceled();
    }

    private async Task RunAsync(CancellationTokenSource cts)
    {
        try
        {
            await ListenAsync(cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            if (!_listeningEstablished && Mode != PostgresListenMode.PollOnly)
            {
                WarnOnce(ProbeReason);
                SetMode(PostgresListenMode.PollOnly);
                _ready.TrySetResult();
            }
            else if (_logger is not null)
            {
                PostgresEventStoreLog.ListenDropped(_logger, ex);
            }
        }
        finally
        {
            cts.Dispose();
        }
    }

    private async Task ListenAsync(CancellationToken cancellationToken)
    {
        if (_store.ForcePollOnly || !_options.NotifyOnAppend)
        {
            SetMode(PostgresListenMode.PollOnly);
            _ready.TrySetResult();
            return;
        }

        var connectionString = string.IsNullOrWhiteSpace(_options.ListenConnectionString)
            ? _options.ConnectionString!
            : _options.ListenConnectionString!;

        if (PostgresPoolerHost.LooksLikeTransactionPooler(connectionString))
        {
            WarnOnce(PoolerReason);
            SetMode(PostgresListenMode.PollOnly);
            _ready.TrySetResult();
            return;
        }

        var delayStart = DelayStart;
        if (delayStart is { IsCompleted: false })
        {
            Volatile.Write(ref _delayingStart, 1);
            try
            {
                await delayStart.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                Volatile.Write(ref _delayingStart, 0);
            }
        }

        var attempt = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            var connection = OpenDedicated(connectionString);
            try
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                Volatile.Write(ref _backendPid, connection.ProcessID);
                connection.Notification += OnNotification;
                await ExecuteListenAsync(connection, cancellationToken).ConfigureAwait(false);

                if (!_listeningEstablished)
                {
                    SetMode(PostgresListenMode.Probing);
                    if (!await ProbeAsync(connection, cancellationToken).ConfigureAwait(false))
                    {
                        WarnOnce(ProbeReason);
                        SetMode(PostgresListenMode.PollOnly);
                        _ready.TrySetResult();
                        return;
                    }

                    _listeningEstablished = true;
                    Interlocked.Exchange(ref _loggedDrop, 0);
                    SetMode(PostgresListenMode.Listening);
                    _ready.TrySetResult();
                }
                else
                {
                    Interlocked.Exchange(ref _loggedDrop, 0);
                    SetMode(PostgresListenMode.Listening);
                    _store.WakeSubscribersForCatchUp();
                }

                attempt = 0;
                await PumpAsync(connection, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                if (!_listeningEstablished)
                {
                    WarnOnce(ProbeReason);
                    SetMode(PostgresListenMode.PollOnly);
                    _ready.TrySetResult();
                    return;
                }

                SetMode(PostgresListenMode.Reconnecting);
                if (Interlocked.Exchange(ref _loggedDrop, 1) == 0 && _logger is not null)
                    PostgresEventStoreLog.ListenDropped(_logger, ex);

                var hold = DelayReconnect;
                if (hold is { IsCompleted: false })
                    await hold.WaitAsync(cancellationToken).ConfigureAwait(false);

                var delay = ReconnectDelayOverride ?? BackoffDelay(attempt);
                attempt++;
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                Volatile.Write(ref _backendPid, 0);
                connection.Notification -= OnNotification;
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async Task<bool> ProbeAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var payload = "probe-" + Guid.NewGuid().ToString("N");
        var seen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _probeSeen = seen;
        _probePayload = payload;
        try
        {
            await using var probe = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand("SELECT pg_notify(@channel, @payload)", probe);
            command.Parameters.AddWithValue("channel", _channel);
            command.Parameters.AddWithValue("payload", payload);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            var timeout = Math.Max(1, ProbeTimeoutMilliseconds);
            var started = Environment.TickCount64;
            while (!seen.Task.IsCompleted)
            {
                var remaining = timeout - (int)(Environment.TickCount64 - started);
                if (remaining <= 0)
                    break;
                if (!await connection.WaitAsync(remaining, cancellationToken).ConfigureAwait(false))
                    break;
            }

            return seen.Task.IsCompleted;
        }
        finally
        {
            _probePayload = null;
            _probeSeen = null;
        }
    }

    private async Task PumpAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var hold = HoldBeforeWait;
            if (hold is { IsCompleted: false })
            {
                Volatile.Write(ref _held, 1);
                try
                {
                    await hold.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    Volatile.Write(ref _held, 0);
                }
            }

            var seen = Volatile.Read(ref _notificationCount);
            Volatile.Write(ref _waiting, 1);
            bool got;
            try
            {
                // A finite wait so a restarted server is noticed. Npgsql's timeout
                // returns false. The following check then fails if the server is gone.
                var waitMs = Math.Max(1, KeepAliveSeconds) * 1000;
                got = await connection.WaitAsync(waitMs, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                Volatile.Write(ref _waiting, 0);
            }

            if (!got)
            {
                await using var ping = new NpgsqlCommand("SELECT 1", connection);
                ping.CommandTimeout = 2;
                await ping.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            // Drain wake-ups already buffered. A 1 millisecond pause ends the burst.
            // The next loop blocks again, so an idle listener does not spin.
            while (await connection.WaitAsync(1, cancellationToken).ConfigureAwait(false))
            {
            }

            if (Volatile.Read(ref _notificationCount) == seen)
                continue;

            Interlocked.Increment(ref _wakeCount);
            var highest = Interlocked.Read(ref _highest);
            if (highest > 0)
                _store.PublishSignal(highest);
            else
                _store.WakeSubscribersForCatchUp();
        }
    }

    private void OnNotification(object? sender, NpgsqlNotificationEventArgs args)
    {
        if (!string.Equals(args.Channel, _channel, StringComparison.Ordinal))
            return;

        var probe = _probePayload;
        if (probe is not null && string.Equals(args.Payload, probe, StringComparison.Ordinal))
        {
            _probeSeen?.TrySetResult();
            return;
        }

        Interlocked.Increment(ref _notificationCount);
        if (!long.TryParse(args.Payload, NumberStyles.Integer, CultureInfo.InvariantCulture, out var position) || position <= 0)
            return;

        long observed;
        do
        {
            observed = Interlocked.Read(ref _highest);
            if (position <= observed)
                return;
        }
        while (Interlocked.CompareExchange(ref _highest, position, observed) != observed);
    }

    private async Task ExecuteListenAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand($"LISTEN {PgIdentifier.Quote(_channel)}", connection);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private void WarnOnce(string reason)
    {
        if (Interlocked.Exchange(ref _warned, 1) != 0)
            return;
        if (_logger is not null)
            PostgresEventStoreLog.ListenUnavailable(_logger, reason);
    }

    private void SetMode(PostgresListenMode mode) => Volatile.Write(ref _mode, (int)mode);

    private NpgsqlConnection OpenDedicated(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = false,
            Multiplexing = false,
            KeepAlive = Math.Max(1, KeepAliveSeconds),
            Enlist = false,
            Timeout = 5,
            CommandTimeout = 15,
            ApplicationName = "lawndart-listen",
            IncludeErrorDetail = true
        };
        return new NpgsqlConnection(builder.ConnectionString);
    }

    private static TaskCompletionSource CreateReady()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
