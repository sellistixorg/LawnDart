using RabbitMQ.Client;

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

    public RabbitMqConnector(RabbitMqTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    public async Task DeclareAsync(string messageType, string subscriptionName, CancellationToken cancellationToken)
    {
        var key = messageType + "\n" + subscriptionName;
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
                await RabbitMqDeclarations.DeclareAsync(channel, messageType, subscriptionName, cancellationToken)
                    .ConfigureAwait(false);
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

            _admin = null;
            var factory = CreateFactory(_options);
            _connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
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
