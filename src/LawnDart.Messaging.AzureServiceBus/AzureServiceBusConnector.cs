using System.Collections.Concurrent;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// Shared Service Bus clients, senders, and topic or subscription declarations.
/// </summary>
internal sealed class AzureServiceBusConnector : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Broker delivery cap on a namespace that allows it.
    /// A scheduled retry completes the current delivery, so this cap is not <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>.
    /// </summary>
    public const int BrokerMaxDeliveryCount = 2000;

    /// <summary>
    /// Highest delivery cap the Service Bus emulator accepts.
    /// Used when the namespace rejects <see cref="BrokerMaxDeliveryCount"/>.
    /// </summary>
    public const int EmulatorMaxDeliveryCount = 10;

    private readonly AzureServiceBusTransportOptions _options;
    private readonly Func<ServiceBusClient> _clientFactory;
    private readonly Func<ServiceBusAdministrationClient> _adminFactory;
    private readonly bool _ownsClient;
    private readonly bool _ownsAdmin;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly SemaphoreSlim _declareLock = new(1, 1);
    private readonly SemaphoreSlim _senderLock = new(1, 1);
    private readonly Dictionary<string, byte> _ready = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new(StringComparer.Ordinal);
    private ServiceBusClient? _client;
    private ServiceBusAdministrationClient? _admin;
    private int _disposed;

    public AzureServiceBusConnector(
        AzureServiceBusTransportOptions options,
        Func<ServiceBusClient> clientFactory,
        Func<ServiceBusAdministrationClient> adminFactory,
        bool ownsClient,
        bool ownsAdmin)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientFactory);
        ArgumentNullException.ThrowIfNull(adminFactory);
        _options = options;
        _clientFactory = clientFactory;
        _adminFactory = adminFactory;
        _ownsClient = ownsClient;
        _ownsAdmin = ownsAdmin;
    }

    public ServiceBusClient Client => GetClient();

    public async Task<ServiceBusSender> SenderAsync(string topic, CancellationToken cancellationToken)
    {
        if (_senders.TryGetValue(topic, out var existing))
            return existing;

        await WaitAsync(_senderLock, cancellationToken).ConfigureAwait(false);
        try
        {
            if (_senders.TryGetValue(topic, out existing))
                return existing;

            var created = GetClient().CreateSender(topic);
            _senders[topic] = created;
            return created;
        }
        finally
        {
            _senderLock.Release();
        }
    }

    public Task EnsureTopicAsync(string topic, CancellationToken cancellationToken)
        => DeclareOnceAsync("topic\n" + topic, ct => CreateTopicAsync(topic, ct), cancellationToken);

    public Task EnsureSubscriptionAsync(string topic, string subscriptionName, CancellationToken cancellationToken)
        => DeclareOnceAsync(
            "subscription\n" + topic + "\n" + subscriptionName,
            async ct =>
            {
                // CreateTopicAsync does not take the declare lock. Calling EnsureTopicAsync here would deadlock.
                await CreateTopicAsync(topic, ct).ConfigureAwait(false);
                MarkReady("topic\n" + topic);

                var admin = GetAdmin();
                if (await admin.SubscriptionExistsAsync(topic, subscriptionName, ct).ConfigureAwait(false))
                    return;

                var create = new CreateSubscriptionOptions(topic, subscriptionName)
                {
                    MaxDeliveryCount = BrokerMaxDeliveryCount,
                    DeadLetteringOnMessageExpiration = false,
                };

                try
                {
                    await CreateSubscriptionAsync(admin, create, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsAlreadyExists(ex))
                {
                }
            },
            cancellationToken);

    private async Task CreateTopicAsync(string topic, CancellationToken cancellationToken)
    {
        if (IsReady("topic\n" + topic))
            return;

        var admin = GetAdmin();
        if (await admin.TopicExistsAsync(topic, cancellationToken).ConfigureAwait(false))
            return;

        try
        {
            await admin.CreateTopicAsync(topic, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsAlreadyExists(ex))
        {
        }
    }

    public void Dispose()
    {
        var dispose = DisposeAsync().AsTask();
        if (!dispose.Wait(_options.OperationTimeout))
        {
            throw new TimeoutException(
                $"Disposing the Azure Service Bus connector did not finish within {_options.OperationTimeout}.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var sender in _senders.Values)
            await sender.DisposeAsync().ConfigureAwait(false);

        if (_ownsClient && _client is not null)
            await _client.DisposeAsync().ConfigureAwait(false);

        if (_ownsAdmin && _admin is not null)
            await DisposeAdminAsync().ConfigureAwait(false);

        _initLock.Dispose();
        _declareLock.Dispose();
        _senderLock.Dispose();
    }

    private ServiceBusClient GetClient()
    {
        if (_client is not null)
            return _client;

        if (!_initLock.Wait(_options.OperationTimeout))
        {
            throw new TimeoutException(
                $"Azure Service Bus waited {_options.OperationTimeout} for a local lock.");
        }

        try
        {
            return _client ??= _clientFactory();
        }
        finally
        {
            _initLock.Release();
        }
    }

    private ServiceBusAdministrationClient GetAdmin()
    {
        if (_admin is not null)
            return _admin;

        if (!_initLock.Wait(_options.OperationTimeout))
        {
            throw new TimeoutException(
                $"Azure Service Bus waited {_options.OperationTimeout} for a local lock.");
        }

        try
        {
            return _admin ??= _adminFactory();
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task DeclareOnceAsync(
        string key,
        Func<CancellationToken, Task> declare,
        CancellationToken cancellationToken)
    {
        if (IsReady(key))
            return;

        await WaitAsync(_declareLock, cancellationToken).ConfigureAwait(false);
        try
        {
            if (IsReady(key))
                return;

            using var timeout = LinkTimeout(cancellationToken);
            await declare(timeout.Token).ConfigureAwait(false);
            MarkReady(key);
        }
        finally
        {
            _declareLock.Release();
        }
    }

    private async Task WaitAsync(SemaphoreSlim gate, CancellationToken cancellationToken)
    {
        if (await gate.WaitAsync(_options.OperationTimeout, cancellationToken).ConfigureAwait(false))
            return;

        throw new TimeoutException(
            $"Azure Service Bus waited {_options.OperationTimeout} for a local lock.");
    }

    private CancellationTokenSource LinkTimeout(CancellationToken cancellationToken)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        source.CancelAfter(_options.OperationTimeout);
        return source;
    }

    private bool IsReady(string key)
    {
        lock (_ready)
            return _ready.ContainsKey(key);
    }

    private void MarkReady(string key)
    {
        lock (_ready)
            _ready[key] = 0;
    }

    private async ValueTask DisposeAdminAsync()
    {
        if (_admin is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
        else if (_admin is IDisposable disposable)
            disposable.Dispose();
    }

    private static async Task CreateSubscriptionAsync(
        ServiceBusAdministrationClient admin,
        CreateSubscriptionOptions create,
        CancellationToken cancellationToken)
    {
        try
        {
            await admin.CreateSubscriptionAsync(create, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsDeliveryCountRejected(ex))
        {
            create.MaxDeliveryCount = EmulatorMaxDeliveryCount;
            await admin.CreateSubscriptionAsync(create, cancellationToken).ConfigureAwait(false);
        }
    }

    private static bool IsDeliveryCountRejected(Exception exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is ArgumentException argument &&
                string.Equals(argument.ParamName, "MaxDeliveryCount", StringComparison.Ordinal))
            {
                return true;
            }

            if (current.Message.Contains("MaxDeliveryCount", StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static bool IsAlreadyExists(Exception exception)
    {
        if (exception is ServiceBusException serviceBus &&
            serviceBus.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            return true;
        }

        return exception is RequestFailedException request && request.Status == 409;
    }
}
