using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.Messaging.Telemetry;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Messaging.Hosting;

/// <summary>
/// Hosted service that drives a single <see cref="IReactor{TEvent}"/> instance.
/// On startup it subscribes to <typeparamref name="TEvent"/> via <see cref="IMessageTransport"/>,
/// applies inbox deduplication via <see cref="IInboxStore"/>, invokes the reactor,
/// then dispatches returned commands via <see cref="ICommandDispatcher"/> (if registered).
/// </summary>
/// <typeparam name="TReactor">The reactor implementation type.</typeparam>
/// <typeparam name="TEvent">The event type the reactor handles.</typeparam>
public sealed class ReactorHostedService<TReactor, TEvent> : BackgroundService
    where TReactor : class, IReactor<TEvent>
    where TEvent : class, IEvent
{
    private readonly TReactor _reactor;
    private readonly IMessageTransport _transport;
    private readonly IInboxStore _inboxStore;
    private readonly ICommandDispatcher? _commandDispatcher;
    private readonly ILogger<ReactorHostedService<TReactor, TEvent>> _logger;

    private static readonly string ReactorTypeName = typeof(TReactor).Name;
    private static readonly string EventTypeName = typeof(TEvent).Name;

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

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => _transport.SubscribeAsync<TEvent>(HandleAsync, stoppingToken);

    private async Task HandleAsync(TEvent @event, MessageContext context, CancellationToken ct)
    {
        var messageId = context.MessageId;

        if (messageId is not null && await _inboxStore.IsProcessedAsync(messageId, ct))
        {
            MessagingTelemetry.RecordInboxDuplicate(EventTypeName);
            _logger.LogDebug(
                "Reactor {Reactor} skipping duplicate message {MessageId} for event {EventType}",
                ReactorTypeName, messageId, EventTypeName);
            return;
        }

        using var activity = ReactorTelemetry.StartHandle(ReactorTypeName, EventTypeName, context);
        var start = TimeProvider.System.GetTimestamp();

        try
        {
            var commands = (await _reactor.ReactAsync(@event, context, ct)).ToList();

            if (_commandDispatcher is not null)
            {
                foreach (var command in commands)
                {
                    await _commandDispatcher.DispatchAsync(command, context.CreateChild(), ct);
                }
            }
            else if (commands.Count > 0)
            {
                _logger.LogWarning(
                    "Reactor {Reactor} emitted {Count} command(s) but no ICommandDispatcher is registered. Commands will not be dispatched.",
                    ReactorTypeName, commands.Count);
            }

            if (messageId is not null)
            {
                await _inboxStore.MarkProcessedAsync(messageId, DateTimeOffset.UtcNow, ct);
            }

            var elapsed = TimeProvider.System.GetElapsedTime(start);
            ReactorTelemetry.RecordHandle(ReactorTypeName, EventTypeName, elapsed, commands.Count);

            _logger.LogDebug(
                "Reactor {Reactor} handled {EventType} in {ElapsedMs:F1}ms, emitted {CommandCount} command(s)",
                ReactorTypeName, EventTypeName, elapsed.TotalMilliseconds, commands.Count);
        }
        catch (Exception ex)
        {
            ReactorTelemetry.RecordError(ReactorTypeName, EventTypeName);
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
            _logger.LogError(ex,
                "Reactor {Reactor} failed while handling {EventType} (MessageId={MessageId})",
                ReactorTypeName, EventTypeName, messageId);
            throw;
        }
    }
}
