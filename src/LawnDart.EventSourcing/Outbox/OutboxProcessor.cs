using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.EventSourcing.Telemetry;
using LawnDart.EventStore;
using LawnDart.Outbox;

namespace LawnDart.EventSourcing.Outbox;

/// <summary>
/// Background service that processes outbox messages.
/// Polls for unprocessed messages and publishes them to external systems.
/// </summary>
public class OutboxProcessor : BackgroundService
{
    private readonly IOutboxWriter _outboxWriter;
    private readonly IOutboxPublisher _outboxPublisher;
    private readonly ILogger<OutboxProcessor> _logger;
    private readonly TimeSpan _pollingInterval;
    private readonly int _batchSize;
    private readonly int _maxAttempts;
    private readonly NewEventsSignal? _newEvents;
    private readonly IHostApplicationLifetime? _lifetime;
    private readonly OutboxSendBarrier? _barrier;
    private readonly CancellationTokenSource _sendStop = new();
    private readonly TaskCompletionSource _sendIdle = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _loopEntered;

    /// <summary>
    /// Initializes a new instance of the OutboxProcessor.
    /// </summary>
    /// <param name="outboxWriter">Outbox writer for reading and updating messages.</param>
    /// <param name="outboxPublisher">Publisher for sending messages to external systems.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="pollingInterval">How often to poll for new messages. Default is 5 seconds.</param>
    /// <param name="batchSize">Number of messages to process per batch. Default is 100.</param>
    /// <param name="maxAttempts">Maximum retry attempts before giving up. Default is 10.</param>
    /// <param name="newEvents">
    /// Optional wake-up shared with the event store. When set, the processor waits on
    /// this signal or <paramref name="pollingInterval"/>, whichever comes first.
    /// When null, it waits for the poll interval only.
    /// </param>
    public OutboxProcessor(
        IOutboxWriter outboxWriter,
        IOutboxPublisher outboxPublisher,
        ILogger<OutboxProcessor> logger,
        TimeSpan? pollingInterval = null,
        int batchSize = 100,
        int maxAttempts = 10,
        NewEventsSignal? newEvents = null)
        : this(outboxWriter, outboxPublisher, logger, null, null, pollingInterval, batchSize, maxAttempts, newEvents)
    {
    }

    /// <summary>
    /// Host registration. Waits until every hosted service has started, then publishes.
    /// Stops publishing when the host begins to shut down, before other hosted services stop.
    /// </summary>
    internal OutboxProcessor(
        IOutboxWriter outboxWriter,
        IOutboxPublisher outboxPublisher,
        ILogger<OutboxProcessor> logger,
        IHostApplicationLifetime? lifetime,
        OutboxSendBarrier? barrier,
        TimeSpan? pollingInterval = null,
        int batchSize = 100,
        int maxAttempts = 10,
        NewEventsSignal? newEvents = null)
    {
        _outboxWriter = outboxWriter ?? throw new ArgumentNullException(nameof(outboxWriter));
        _outboxPublisher = outboxPublisher ?? throw new ArgumentNullException(nameof(outboxPublisher));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _lifetime = lifetime;
        _barrier = barrier;
        _pollingInterval = pollingInterval ?? TimeSpan.FromSeconds(5);
        _batchSize = batchSize;
        _maxAttempts = maxAttempts;
        _newEvents = newEvents;
        lifetime?.ApplicationStopping.Register(StopBeforeListeners);
    }

    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Interlocked.Exchange(ref _loopEntered, 1);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _sendStop.Token);
        var send = linked.Token;
        try
        {
            if (_lifetime is not null)
                await WaitForHostStartAsync(_lifetime, send).ConfigureAwait(false);
            if (_barrier is not null)
                await _barrier.WaitAsync(send).ConfigureAwait(false);

            _logger.LogInformation("Outbox processor starting");

            while (!send.IsCancellationRequested)
            {
                var ticket = _newEvents?.TakeTicket();
                try
                {
                    // Finish the batch already in hand. Shutdown must not cancel it,
                    // because listeners are still subscribed until this method goes idle.
                    await ProcessBatchAsync(_lifetime is null ? send : CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "Error processing outbox batch");
                }

                if (send.IsCancellationRequested)
                    break;

                try
                {
                    if (_newEvents is { } signal && ticket is { } since)
                        await signal.WaitAsync(since, _pollingInterval, send).ConfigureAwait(false);
                    else
                        await Task.Delay(_pollingInterval, send).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (send.IsCancellationRequested)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (send.IsCancellationRequested)
        {
        }
        finally
        {
            _sendIdle.TrySetResult();
            _logger.LogInformation("Outbox processor stopping");
        }
    }

    private static async Task WaitForHostStartAsync(IHostApplicationLifetime lifetime, CancellationToken cancellationToken)
    {
        if (lifetime.ApplicationStarted.IsCancellationRequested)
            return;

        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var ready = lifetime.ApplicationStarted.Register(() => started.TrySetResult());
        using var cancel = cancellationToken.Register(() => started.TrySetCanceled(cancellationToken));
        if (lifetime.ApplicationStarted.IsCancellationRequested)
            started.TrySetResult();
        await started.Task.ConfigureAwait(false);
    }

    private void StopBeforeListeners()
    {
        _sendStop.Cancel();
        if (Volatile.Read(ref _loopEntered) == 0 || _sendIdle.Task.IsCompleted)
            return;

        try
        {
            if (!_sendIdle.Task.Wait(TimeSpan.FromSeconds(30)))
            {
                _logger.LogDebug("Outbox processor was still publishing when the host stopped listeners.");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "Outbox processor was still publishing when the host stopped listeners.");
        }
    }
    
    /// <summary>
    /// Processes a batch of outbox messages.
    /// </summary>
    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        var messages = await _outboxWriter.GetUnprocessedAsync(_batchSize, cancellationToken).ConfigureAwait(false);
        
        if (messages.Count == 0)
        {
            return;
        }
        
        _logger.LogDebug("Processing {Count} outbox messages", messages.Count);
        
        foreach (var message in messages)
        {
            if (message.Attempts >= _maxAttempts)
            {
                _logger.LogWarning(
                    "Outbox message {MessageId} has exceeded max attempts ({MaxAttempts}), dead-lettering",
                    message.Id, _maxAttempts);
                OutboxTelemetry.RecordDeadLettered(message.EventType);
                await _outboxWriter.MarkAsDeadLetteredAsync(message.Id, cancellationToken).ConfigureAwait(false);
                continue;
            }
            
            try
            {
                await _outboxPublisher.PublishAsync(message, cancellationToken).ConfigureAwait(false);
                await _outboxWriter.MarkAsProcessedAsync(message.Id, cancellationToken).ConfigureAwait(false);
                
                _logger.LogDebug("Successfully published outbox message {MessageId}", message.Id);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to publish outbox message {MessageId}", message.Id);
                OutboxTelemetry.RecordPublishFailure(message.EventType);
                await _outboxWriter.RecordFailureAsync(message.Id, ex.Message, cancellationToken).ConfigureAwait(false);
                if (message.Attempts + 1 >= _maxAttempts)
                {
                    _logger.LogWarning(
                        "Outbox message {MessageId} reached max attempts ({MaxAttempts}), dead-lettering",
                        message.Id, _maxAttempts);
                    OutboxTelemetry.RecordDeadLettered(message.EventType);
                    await _outboxWriter.MarkAsDeadLetteredAsync(message.Id, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }
}
