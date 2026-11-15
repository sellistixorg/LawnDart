using LawnDart.Publishing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.Outbox;

/// <summary>
/// Checks published event types for a listener after every hosted service has started,
/// and before <see cref="IHostApplicationLifetime.ApplicationStarted"/>.
/// </summary>
internal sealed class OutboundListenerCheck : IHostedService, IHostedLifecycleService
{
    private readonly OutboundListenerPlan _plan;
    private readonly IMessageTransport _transport;
    private readonly MessageTransportOptions _options;
    private readonly IServiceProvider _services;
    private readonly ILogger<OutboundListenerCheck> _logger;

    public OutboundListenerCheck(
        OutboundListenerPlan plan,
        IMessageTransport transport,
        IOptions<MessageTransportOptions> options,
        IServiceProvider services,
        ILogger<OutboundListenerCheck> logger)
    {
        _plan = plan;
        _transport = transport;
        _options = options.Value;
        _services = services;
        _logger = logger;
    }

    /// <inheritdoc />
    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StartedAsync(CancellationToken cancellationToken) => CheckAsync(cancellationToken);

    /// <inheritdoc />
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task CheckAsync(CancellationToken cancellationToken)
    {
        if (_transport is not IOutboundListenerProbe probe)
        {
            LogUnavailable(
                "Startup listener check is not available for transport {Transport}. The publish-time check still applies.",
                _transport.GetType().Name);
            return;
        }

        var subscribed = _services.GetServices<MessageSubscription>()
            .Select(subscription => subscription.MessageType)
            .ToHashSet();
        var missing = new List<string>();
        foreach (var messageType in _plan.Resolve(_services))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (subscribed.Contains(messageType))
                continue;

            string declaredName;
            try
            {
                declaredName = MessageTypeNames.Require(messageType);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                LogUnavailable(
                    ex,
                    "Could not check listeners for {MessageType}. Startup continues.",
                    messageType.FullName);
                continue;
            }

            bool listening;
            try
            {
                listening = await probe.HasListenerAsync(messageType, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                LogUnavailable(
                    ex,
                    "Could not check listeners for {MessageType}. Startup continues.",
                    declaredName);
                continue;
            }

            if (!listening)
                missing.Add(declaredName);
        }

        if (missing.Count == 0)
            return;

        if (_options.OnMissingListenerAtStartup == MissingListenerBehavior.Throw)
        {
            throw new InvalidOperationException(
                "Published message " + string.Join(", ", missing) + " has no listener.");
        }

        foreach (var declaredName in missing)
        {
            _logger.LogWarning(
                "Published message {MessageType} has no listener. The outbox keeps the row and tries it again until the attempt limit.",
                declaredName);
        }
    }

    private void LogUnavailable(string message, params object?[] args)
        => LogUnavailable(exception: null, message, args);

    private void LogUnavailable(Exception? exception, string message, params object?[] args)
    {
        var level = _options.OnMissingListenerAtStartup == MissingListenerBehavior.Throw
            ? LogLevel.Warning
            : LogLevel.Information;
        _logger.Log(level, exception, message, args);
    }
}

/// <summary>
/// Published types collected from every <c>AddMessageTransportOutboxPublisher</c> call.
/// </summary>
internal sealed class OutboundListenerPlan
{
    private readonly object _gate = new();
    private readonly List<Type> _explicit = [];
    private bool _includeCatalog;

    public void Add(IReadOnlyCollection<Type> types)
    {
        lock (_gate)
        {
            if (types.Count == 0)
                _includeCatalog = true;
            else
                _explicit.AddRange(types);
        }
    }

    public IReadOnlyList<Type> Resolve(IServiceProvider services)
    {
        lock (_gate)
        {
            if (!_includeCatalog)
                return _explicit.Distinct().ToArray();

            IEnumerable<Type> catalog = [];
            if (services.GetService<PublishedEventCatalog>() is { } found)
                catalog = found.PublishedTypes;
            return _explicit.Concat(catalog).Distinct().ToArray();
        }
    }
}
