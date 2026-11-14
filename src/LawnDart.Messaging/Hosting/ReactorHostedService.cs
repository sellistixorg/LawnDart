using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.Messaging.Telemetry;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Messaging.Hosting;

/// <summary>
/// Hosted service that drives a single <see cref="IReactor{TEvent}"/> instance.
/// On startup it subscribes to <typeparamref name="TEvent"/> via <see cref="IMessageTransport"/>,
/// applies inbox deduplication via <see cref="IInboxStore"/> (a hash of this reactor's type name plus the message id),
/// invokes the reactor, then dispatches returned commands via <see cref="ICommandDispatcher"/> (if registered).
/// </summary>
/// <typeparam name="TReactor">The reactor implementation type.</typeparam>
/// <typeparam name="TEvent">The event type the reactor handles.</typeparam>
public sealed class ReactorHostedService<TReactor, TEvent> : BackgroundService
    where TReactor : class, IReactor<TEvent>
    where TEvent : class, IEvent
{
    private readonly TReactor? _reactor;
    private readonly IServiceScopeFactory? _scopeFactory;
    private readonly IMessageTransport _transport;
    private readonly IInboxStore _inboxStore;
    private readonly ICommandDispatcher? _commandDispatcher;
    private readonly ILogger<ReactorHostedService<TReactor, TEvent>> _logger;

    private static readonly string ReactorTypeName = typeof(TReactor).Name;
    private string _eventTypeName = "";
    private CancellationTokenSource? _subscriptionStop;
    private Task? _subscription;

    public ReactorHostedService(
        TReactor reactor,
        IMessageTransport transport,
        IInboxStore inboxStore,
        ILogger<ReactorHostedService<TReactor, TEvent>> logger,
        ICommandDispatcher? commandDispatcher = null)
    {
        _reactor = reactor;
        _transport = transport;
        _inboxStore = inboxStore;
        _logger = logger;
        _commandDispatcher = commandDispatcher;
    }

    /// <summary>
    /// Creates a host that resolves <typeparamref name="TReactor"/> from a new scope for each message.
    /// The scope is disposed when handling finishes.
    /// </summary>
    internal ReactorHostedService(
        IServiceScopeFactory scopeFactory,
        IMessageTransport transport,
        IInboxStore inboxStore,
        ILogger<ReactorHostedService<TReactor, TEvent>> logger)
    {
        _scopeFactory = scopeFactory;
        _transport = transport;
        _inboxStore = inboxStore;
        _logger = logger;
    }

    /// <summary>
    /// Subscribes to <typeparamref name="TEvent"/>.
    /// A missing <c>[EventTypeName]</c> is logged and thrown to the caller.
    /// </summary>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            _eventTypeName = MessageTypeNames.Require(typeof(TEvent));
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Reactor {Reactor} failed to start. Event type {EventType} must declare [EventTypeName].",
                ReactorTypeName,
                typeof(TEvent).FullName);
            throw;
        }

        // Subscribe before StartAsync returns. BackgroundService runs ExecuteAsync
        // on another thread, after the host may already have started.
        _subscriptionStop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _subscription = _transport.SubscribeAsync<TEvent>(HandleAsync, _subscriptionStop.Token);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_subscription is null || _subscriptionStop is null)
            return;

        using var stop = stoppingToken.Register(static state =>
        {
            var source = (CancellationTokenSource)state!;
            try
            {
                source.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }, _subscriptionStop);
        try
        {
            await _subscription.ConfigureAwait(false);
        }
        finally
        {
            _subscriptionStop.Dispose();
            _subscriptionStop = null;
        }
    }

    private async Task HandleAsync(TEvent @event, MessageContext context, CancellationToken ct)
    {
        var messageId = context.MessageId;
        var inboxKey = messageId is null ? null : InboxConsumerKey.ForReactor(typeof(TReactor), messageId);
        var claimed = false;

        if (inboxKey is not null)
        {
            claimed = await _inboxStore.TryClaimAsync(inboxKey, ct).ConfigureAwait(false);
            if (!claimed)
            {
                if (await _inboxStore.IsProcessedAsync(inboxKey, ct).ConfigureAwait(false))
                {
                    MessagingTelemetry.RecordInboxDuplicate(_eventTypeName);
                    _logger.LogDebug(
                        "Reactor {Reactor} skipping duplicate message {MessageId} for event {EventType}",
                        ReactorTypeName, messageId, _eventTypeName);
                    return;
                }

                ReactorTelemetry.RecordError(ReactorTypeName, _eventTypeName);
                _logger.LogDebug(
                    "Reactor {Reactor} could not claim message {MessageId} for event {EventType}. Another attempt still holds the claim.",
                    ReactorTypeName, messageId, _eventTypeName);
                throw new InvalidOperationException(
                    $"Reactor {ReactorTypeName} could not claim message '{messageId}' for '{_eventTypeName}'. Another attempt still holds the claim.");
            }
        }

        AsyncServiceScope scope = default;
        var scoped = false;
        using var activity = ReactorTelemetry.StartHandle(ReactorTypeName, _eventTypeName, context);
        var start = TimeProvider.System.GetTimestamp();
        System.Runtime.ExceptionServices.ExceptionDispatchInfo? failure = null;

        try
        {
            TReactor reactor;
            ICommandDispatcher? dispatcher;
            if (_scopeFactory is not null)
            {
                scope = _scopeFactory.CreateAsyncScope();
                scoped = true;
                reactor = scope.ServiceProvider.GetRequiredService<TReactor>();
                dispatcher = scope.ServiceProvider.GetService<ICommandDispatcher>();
            }
            else
            {
                reactor = _reactor!;
                dispatcher = _commandDispatcher;
            }

            var commands = (await reactor.ReactAsync(@event, context, ct).ConfigureAwait(false)).ToList();

            if (dispatcher is not null)
            {
                foreach (var command in commands)
                {
                    await dispatcher.DispatchAsync(command, context.CreateChild(), ct).ConfigureAwait(false);
                }
            }
            else if (commands.Count > 0)
            {
                _logger.LogWarning(
                    "Reactor {Reactor} emitted {Count} command(s) but no ICommandDispatcher is registered. Commands will not be dispatched.",
                    ReactorTypeName, commands.Count);
            }

            if (inboxKey is not null)
            {
                await _inboxStore.MarkProcessedAsync(inboxKey, DateTimeOffset.UtcNow, ct).ConfigureAwait(false);
            }

            var elapsed = TimeProvider.System.GetElapsedTime(start);
            ReactorTelemetry.RecordHandle(ReactorTypeName, _eventTypeName, elapsed, commands.Count);

            _logger.LogDebug(
                "Reactor {Reactor} handled {EventType} in {ElapsedMs:F1}ms, emitted {CommandCount} command(s)",
                ReactorTypeName, _eventTypeName, elapsed.TotalMilliseconds, commands.Count);
        }
        catch (Exception ex)
        {
            failure = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex);
            if (claimed && inboxKey is not null)
            {
                try
                {
                    await _inboxStore.ReleaseClaimAsync(inboxKey, ct).ConfigureAwait(false);
                }
                catch (Exception releaseEx) when (releaseEx is not OperationCanceledException)
                {
                    _logger.LogDebug(releaseEx, "Reactor {Reactor} failed to release inbox claim {MessageId}", ReactorTypeName, messageId);
                }
            }

            ReactorTelemetry.RecordError(ReactorTypeName, _eventTypeName);
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
            _logger.LogError(ex,
                "Reactor {Reactor} failed while handling {EventType} (MessageId={MessageId})",
                ReactorTypeName, _eventTypeName, messageId);
        }
        finally
        {
            if (scoped)
            {
                try
                {
                    await scope.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception disposeEx)
                {
                    if (failure is null)
                    {
                        _logger.LogWarning(
                            disposeEx,
                            "Reactor {Reactor} failed to dispose the message scope",
                            ReactorTypeName);
                    }
                    else
                    {
                        _logger.LogDebug(
                            disposeEx,
                            "Reactor {Reactor} failed to dispose the message scope",
                            ReactorTypeName);
                    }

                    failure ??= System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(disposeEx);
                }
            }
        }

        failure?.Throw();
    }
}
