using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.Messaging.Telemetry;
using LawnDart.Patterns.EventProcessing;

namespace LawnDart.Messaging.Hosting;

/// <summary>
/// Hosted service that drives a single <see cref="IEventProcessor{TEvent}"/> instance.
/// Subscribes to <typeparamref name="TEvent"/>, applies inbox deduplication, invokes the processor,
/// then publishes returned derived events back through the transport.
/// </summary>
/// <typeparam name="TProcessor">The event processor implementation type.</typeparam>
/// <typeparam name="TEvent">The input event type the processor handles.</typeparam>
public sealed class EventProcessorHostedService<TProcessor, TEvent> : BackgroundService
    where TProcessor : class, IEventProcessor<TEvent>
    where TEvent : class, IEvent
{
    private readonly TProcessor _processor;
    private readonly IMessageTransport _transport;
    private readonly IInboxStore _inboxStore;
    private readonly ILogger<EventProcessorHostedService<TProcessor, TEvent>> _logger;

    private static readonly string ProcessorTypeName = typeof(TProcessor).Name;
    private static readonly string EventTypeName = typeof(TEvent).Name;

    public EventProcessorHostedService(
        TProcessor processor,
        IMessageTransport transport,
        IInboxStore inboxStore,
        ILogger<EventProcessorHostedService<TProcessor, TEvent>> logger)
    {
        _processor = processor;
        _transport = transport;
        _inboxStore = inboxStore;
        _logger = logger;
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
                "EventProcessor {Processor} skipping duplicate message {MessageId} for event {EventType}",
                ProcessorTypeName, messageId, EventTypeName);
            return;
        }

        using var activity = EventProcessorTelemetry.StartProcess(ProcessorTypeName, EventTypeName);
        var start = TimeProvider.System.GetTimestamp();

        try
        {
            var derivedEvents = (await _processor.ProcessAsync(@event, context, ct)).ToList();

            foreach (var derived in derivedEvents)
            {
                // Use runtime-type dispatch so the concrete type drives generic resolution,
                // ensuring the correct TMessage subscription bucket is found.
                await _transport.PublishEventAsync(derived, context.CreateChild(), ct);
            }

            if (messageId is not null)
            {
                await _inboxStore.MarkProcessedAsync(messageId, DateTimeOffset.UtcNow, ct);
            }

            var elapsed = TimeProvider.System.GetElapsedTime(start);
            EventProcessorTelemetry.RecordProcess(ProcessorTypeName, EventTypeName, elapsed, derivedEvents.Count);

            _logger.LogDebug(
                "EventProcessor {Processor} handled {EventType} in {ElapsedMs:F1}ms, emitted {EventCount} derived event(s)",
                ProcessorTypeName, EventTypeName, elapsed.TotalMilliseconds, derivedEvents.Count);
        }
        catch (Exception ex)
        {
            EventProcessorTelemetry.RecordError(ProcessorTypeName, EventTypeName);
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
            _logger.LogError(ex,
                "EventProcessor {Processor} failed while handling {EventType} (MessageId={MessageId})",
                ProcessorTypeName, EventTypeName, messageId);
            throw;
        }
    }
}
