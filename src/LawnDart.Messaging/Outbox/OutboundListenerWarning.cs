using LawnDart.Outbox;
using LawnDart.Publishing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace LawnDart.Messaging.Outbox;

/// <summary>
/// Logs one warning for each published event type that has no listener.
/// Runs after hosted services have started, and before the outbox publishes.
/// The host still starts. There is no fail-fast switch.
/// </summary>
internal sealed class OutboundListenerWarning : BackgroundService
{
    private readonly IMessageTransport _transport;
    private readonly IServiceProvider _services;
    private readonly Type[] _explicitTypes;
    private readonly ILogger<OutboundListenerWarning> _logger;

    public OutboundListenerWarning(
        OutboxSendBarrier barrier,
        IMessageTransport transport,
        IServiceProvider services,
        Type[] explicitTypes,
        ILogger<OutboundListenerWarning> logger)
    {
        _transport = transport;
        _services = services;
        _explicitTypes = explicitTypes;
        _logger = logger;
        barrier.SetCheck(WarnAsync);
    }

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.CompletedTask;

    private async Task WarnAsync(CancellationToken cancellationToken)
    {
        if (_transport is not IOutboundListenerProbe probe)
        {
            _logger.LogDebug(
                "Transport {Transport} does not report listeners. Startup will not warn for a missing listener.",
                _transport.GetType().Name);
            return;
        }

        foreach (var messageType in Types())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var declaredName = MessageTypeNames.Require(messageType);
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
                _logger.LogDebug(ex, "Could not check listeners for {MessageType}.", declaredName);
                continue;
            }

            if (listening)
                continue;

            _logger.LogWarning(
                "Published message {MessageType} has no listener. The outbox keeps the row and tries it again until the attempt limit.",
                declaredName);
        }
    }

    private IReadOnlyCollection<Type> Types()
    {
        if (_explicitTypes.Length > 0)
            return _explicitTypes;

        return _services.GetService(typeof(PublishedEventCatalog)) is PublishedEventCatalog catalog
            ? catalog.PublishedTypes
            : [];
    }
}
