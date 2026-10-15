using System.Collections.Concurrent;
using System.Globalization;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.Messaging.AzureServiceBus;

/// <summary>
/// Shared Service Bus clients, senders, and topic or subscription declarations.
/// </summary>
internal sealed class AzureServiceBusConnector : IDisposable, IAsyncDisposable
{
    private readonly AzureServiceBusTransportOptions _options;
    private readonly Func<ServiceBusClient> _clientFactory;
    private readonly Func<ServiceBusAdministrationClient> _adminFactory;
    private readonly bool _ownsClient;
    private readonly bool _ownsAdmin;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly SemaphoreSlim _declareLock = new(1, 1);
    private readonly SemaphoreSlim _senderLock = new(1, 1);
    private readonly Dictionary<string, byte> _ready = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<string>> _acceptedTypes = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new(StringComparer.Ordinal);
    private readonly ILogger _logger;
    private ServiceBusClient? _client;
    private ServiceBusAdministrationClient? _admin;
    private int _disposed;

    public AzureServiceBusConnector(
        AzureServiceBusTransportOptions options,
        Func<ServiceBusClient> clientFactory,
        Func<ServiceBusAdministrationClient> adminFactory,
        bool ownsClient,
        bool ownsAdmin,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientFactory);
        ArgumentNullException.ThrowIfNull(adminFactory);
        _options = options;
        _clientFactory = clientFactory;
        _adminFactory = adminFactory;
        _ownsClient = ownsClient;
        _ownsAdmin = ownsAdmin;
        _logger = logger ?? NullLogger.Instance;
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

    public Task EnsureSubscriptionAsync(
        string topic,
        string subscriptionName,
        IReadOnlyCollection<string> messageTypes,
        CancellationToken cancellationToken)
    {
        var requested = Normalize(messageTypes);
        var typeKey = string.Join("\n", requested);
        return DeclareOnceAsync(
            "subscription\n" + topic + "\n" + subscriptionName + "\n" + typeKey,
            async ct =>
            {
                // CreateTopicAsync does not take the declare lock. Calling EnsureTopicAsync here would deadlock.
                await CreateTopicAsync(topic, ct).ConfigureAwait(false);
                MarkReady("topic\n" + topic);

                var admin = GetAdmin();
                if (!await admin.SubscriptionExistsAsync(topic, subscriptionName, ct).ConfigureAwait(false))
                {
                    var initial = AcceptTypes(topic, subscriptionName, requested, []);
                    var create = new CreateSubscriptionOptions(topic, subscriptionName)
                    {
                        MaxDeliveryCount = _options.BrokerMaxDeliveryCount,
                        DeadLetteringOnMessageExpiration = false,
                    };
                    var rule = new CreateRuleOptions(
                        AzureServiceBusHeaders.RouteRuleName,
                        new SqlRuleFilter(AzureServiceBusHeaders.RouteFilter(subscriptionName, initial)));

                    try
                    {
                        await admin.CreateSubscriptionAsync(create, rule, ct).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (IsAlreadyExists(ex))
                    {
                    }
                }

                await EnsureRouteRuleAsync(admin, topic, subscriptionName, requested, ct).ConfigureAwait(false);
            },
            cancellationToken);
    }

    private async Task EnsureRouteRuleAsync(
        ServiceBusAdministrationClient admin,
        string topic,
        string subscriptionName,
        IReadOnlyList<string> requested,
        CancellationToken cancellationToken)
    {
        var rules = await ListRulesAsync(admin, topic, subscriptionName, cancellationToken).ConfigureAwait(false);
        var installed = new List<string>();
        foreach (var rule in rules)
        {
            if (!IsRouteRule(rule.Name) || rule.Filter is not SqlRuleFilter sql)
                continue;

            installed.AddRange(AzureServiceBusHeaders.EventTypesInSql(sql.SqlExpression));
        }

        var accepted = AcceptTypes(topic, subscriptionName, requested, installed);
        var expression = AzureServiceBusHeaders.RouteFilter(subscriptionName, accepted);
        var routes = rules.Where(rule => IsRouteRule(rule.Name)).ToArray();
        var match = routes.FirstOrDefault(rule => rule.Filter is SqlRuleFilter sql && sql.SqlExpression == expression);
        if (match is null)
        {
            var name = NextRouteRuleName(routes.Select(rule => rule.Name));
            await admin.CreateRuleAsync(
                topic,
                subscriptionName,
                new CreateRuleOptions(name, new SqlRuleFilter(expression)),
                cancellationToken).ConfigureAwait(false);
            foreach (var rule in routes)
            {
                await DeleteRuleAsync(admin, topic, subscriptionName, rule.Name, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        else
        {
            foreach (var rule in routes)
            {
                if (rule.Name == match.Name)
                    continue;

                await DeleteRuleAsync(admin, topic, subscriptionName, rule.Name, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        var fallback = rules.FirstOrDefault(rule => rule.Name == "$Default");
        if (fallback?.Filter is TrueRuleFilter)
        {
            await DeleteRuleAsync(admin, topic, subscriptionName, "$Default", cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private string[] AcceptTypes(
        string topic,
        string subscriptionName,
        IReadOnlyList<string> requested,
        IReadOnlyList<string> installed)
    {
        var key = topic + "\n" + subscriptionName;
        lock (_acceptedTypes)
        {
            if (!_acceptedTypes.TryGetValue(key, out var set))
            {
                set = new HashSet<string>(StringComparer.Ordinal);
                _acceptedTypes[key] = set;
            }

            foreach (var name in requested)
                set.Add(name);
            foreach (var name in installed)
            {
                if (!string.IsNullOrWhiteSpace(name))
                    set.Add(name);
            }

            return set.OrderBy(static name => name, StringComparer.Ordinal).ToArray();
        }
    }

    private static bool IsRouteRule(string name)
        => name.StartsWith(AzureServiceBusHeaders.RouteRuleName, StringComparison.Ordinal);

    private static string NextRouteRuleName(IEnumerable<string> existing)
    {
        var taken = new HashSet<string>(existing, StringComparer.Ordinal);
        if (!taken.Contains(AzureServiceBusHeaders.RouteRuleName))
            return AzureServiceBusHeaders.RouteRuleName;

        for (var n = 2; n < 1000; n++)
        {
            var name = AzureServiceBusHeaders.RouteRuleName + "-" + n.ToString(CultureInfo.InvariantCulture);
            if (!taken.Contains(name))
                return name;
        }

        throw new InvalidOperationException("Azure Service Bus could not allocate a route rule name.");
    }

    private static string[] Normalize(IReadOnlyCollection<string> messageTypes)
        => messageTypes
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();

    private static async Task<List<RuleProperties>> ListRulesAsync(
        ServiceBusAdministrationClient admin,
        string topic,
        string subscriptionName,
        CancellationToken cancellationToken)
    {
        var rules = new List<RuleProperties>();
        await foreach (var rule in admin.GetRulesAsync(topic, subscriptionName, cancellationToken).ConfigureAwait(false))
            rules.Add(rule);
        return rules;
    }

    private static async Task DeleteRuleAsync(
        ServiceBusAdministrationClient admin,
        string topic,
        string subscriptionName,
        string ruleName,
        CancellationToken cancellationToken)
    {
        try
        {
            await admin.DeleteRuleAsync(topic, subscriptionName, ruleName, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsAlreadyExists(ex) || IsNotFound(ex))
        {
        }
    }

    private static async Task<RuleProperties?> TryGetRuleAsync(
        ServiceBusAdministrationClient admin,
        string topic,
        string subscriptionName,
        string ruleName,
        CancellationToken cancellationToken)
    {
        try
        {
            return (await admin.GetRuleAsync(topic, subscriptionName, ruleName, cancellationToken).ConfigureAwait(false)).Value;
        }
        catch (Exception ex) when (IsNotFound(ex))
        {
            return null;
        }
    }

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

    /// <summary>
    /// Closes senders and the clients this connector opened.
    /// Blocks the caller for up to <see cref="AzureServiceBusTransportOptions.OperationTimeout"/>.
    /// Prefer <see cref="DisposeAsync"/>. When the wait ends first, cleanup can still be running.
    /// </summary>
    public void Dispose()
    {
        var dispose = Task.Run(async () => await DisposeAsync().ConfigureAwait(false));
        if (!dispose.Wait(_options.OperationTimeout))
        {
            _logger.LogWarning(
                "Disposing the Azure Service Bus connector did not finish within {Timeout}. Prefer DisposeAsync.",
                _options.OperationTimeout);
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

    private static bool IsNotFound(Exception exception) =>
        Matches(exception, ServiceBusFailureReason.MessagingEntityNotFound, 404);

    private static bool IsAlreadyExists(Exception exception) =>
        Matches(exception, ServiceBusFailureReason.MessagingEntityAlreadyExists, 409);

    private static bool Matches(Exception exception, ServiceBusFailureReason reason, int status)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is ServiceBusException serviceBus && serviceBus.Reason == reason)
                return true;

            if (current is RequestFailedException request && request.Status == status)
                return true;
        }

        return false;
    }
}
