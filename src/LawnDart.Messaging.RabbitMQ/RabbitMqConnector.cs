using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// One RabbitMQ connection shared by publish, consume, and topology setup.
/// </summary>
internal sealed class RabbitMqConnector : IDisposable, IAsyncDisposable
{
    private readonly RabbitMqTransportOptions _options;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _declareLock = new(1, 1);
    private readonly Dictionary<string, byte> _ready = new(StringComparer.Ordinal);
    private IConnection? _connection;
    private IChannel? _admin;
    private bool _automaticRecovery = true;

    public RabbitMqConnector(RabbitMqTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    public Task DeclareExchangeAsync(string messageType, CancellationToken cancellationToken)
        => DeclareOnceAsync(
            "exchange\n" + messageType,
            (channel, ct) => RabbitMqDeclarations.DeclareExchangeAsync(channel, messageType, ct),
            cancellationToken);

    public Task DeclareSubscriptionAsync(string messageType, string subscriptionName, CancellationToken cancellationToken)
        => DeclareOnceAsync(
            "subscription\n" + messageType + "\n" + subscriptionName,
            (channel, ct) => RabbitMqDeclarations.DeclareSubscriptionAsync(
                channel,
                messageType,
                subscriptionName,
                _options.RetryDelay,
                ct),
            cancellationToken);

    private async Task DeclareOnceAsync(
        string key,
        Func<IChannel, CancellationToken, Task> declare,
        CancellationToken cancellationToken)
    {
        if (IsReady(key))
            return;

        await _declareLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (IsReady(key))
                return;

            try
            {
                var channel = await AdminAsync(cancellationToken).ConfigureAwait(false);
                await declare(channel, cancellationToken).ConfigureAwait(false);
                lock (_ready)
                    _ready[key] = 0;
            }
            catch
            {
                _admin = null;
                throw;
            }
        }
        finally
        {
            _declareLock.Release();
        }
    }

    public async Task<IChannel> CreateChannelAsync(CreateChannelOptions options, CancellationToken cancellationToken)
    {
        var connection = await ConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        await _declareLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_admin is not null)
            {
                await _admin.DisposeAsync().ConfigureAwait(false);
                _admin = null;
            }
        }
        finally
        {
            _declareLock.Release();
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
            _connection = null;
        }
    }

    private bool IsReady(string key)
    {
        lock (_ready)
            return _ready.ContainsKey(key);
    }

    private async Task<IChannel> AdminAsync(CancellationToken cancellationToken)
    {
        if (_admin is { IsOpen: true })
            return _admin;

        var connection = await ConnectionAsync(cancellationToken).ConfigureAwait(false);
        _admin = await connection.CreateChannelAsync(
            new CreateChannelOptions(false, false, null, 1),
            cancellationToken).ConfigureAwait(false);
        return _admin;
    }

    private async Task<IConnection> ConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
            return _connection;

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection is { IsOpen: true })
                return _connection;

            if (_connection is not null && _automaticRecovery && IsRecovering(_connection))
            {
                await WaitForRecoveryAsync(_connection, cancellationToken).ConfigureAwait(false);
                if (_connection.IsOpen)
                    return _connection;
            }

            _admin = null;
            if (_connection is not null)
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
                _connection = null;
            }

            var factory = CreateFactory(_options);
            _automaticRecovery = factory.AutomaticRecoveryEnabled;
            _connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private static bool IsRecovering(IConnection connection)
        => connection.CloseReason is null || connection.CloseReason.Initiator != ShutdownInitiator.Application;

    private static async Task WaitForRecoveryAsync(IConnection connection, CancellationToken cancellationToken)
    {
        if (connection.IsOpen)
            return;

        var opened = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task OnRecovered(object sender, AsyncEventArgs args)
        {
            opened.TrySetResult();
            return Task.CompletedTask;
        }

        connection.RecoverySucceededAsync += OnRecovered;
        try
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(200));
            while (!connection.IsOpen && !opened.Task.IsCompleted)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            connection.RecoverySucceededAsync -= OnRecovered;
        }
    }

    internal static ConnectionFactory CreateFactory(RabbitMqTransportOptions options)
    {
        var factory = new ConnectionFactory();
        if (!string.IsNullOrWhiteSpace(options.Uri))
        {
            factory.Uri = new Uri(options.Uri);
            return factory;
        }

        factory.HostName = options.HostName;
        factory.Port = options.Port;
        factory.UserName = options.UserName;
        factory.Password = options.Password;
        factory.VirtualHost = options.VirtualHost;
        return factory;
    }
}
