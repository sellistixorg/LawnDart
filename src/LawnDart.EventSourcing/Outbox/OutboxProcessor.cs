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
    private readonly CancellationTokenSource _sendStop = new();
    private readonly TaskCompletionSource _sendIdle = new(TaskCreationOptions.RunContinuationsAsynchronously);

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
        : this(outboxWriter, outboxPublisher, logger, null, pollingInterval, batchSize, maxAttempts, newEvents)
    {
    }

    /// <summary>
    /// Host registration. Waits until every hosted service has started, then publishes.
    /// </summary>
    internal OutboxProcessor(
        IOutboxWriter outboxWriter,
        IOutboxPublisher outboxPublisher,
        ILogger<OutboxProcessor> logger,
        IHostApplicationLifetime? lifetime,
        TimeSpan? pollingInterval = null,
        int batchSize = 100,
        int maxAttempts = 10,
        NewEventsSignal? newEvents = null)
    {
        _outboxWriter = outboxWriter ?? throw new ArgumentNullException(nameof(outboxWriter));
        _outboxPublisher = outboxPublisher ?? throw new ArgumentNullException(nameof(outboxPublisher));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _lifetime = lifetime;
        _pollingInterval = pollingInterval ?? TimeSpan.FromSeconds(5);
        _batchSize = batchSize;
        _maxAttempts = maxAttempts;
        _newEvents = newEvents;
    }

    /// <summary>Stops the loop from starting another row. The current row finishes.</summary>
    internal void RequestStop()
    {
        try
        {
            _sendStop.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    /// <summary>
    /// Completes when the loop is idle, or when <paramref name="cancellationToken"/> fires.
    /// A fired token is the host shutdown timeout. The current row is left running.
    /// </summary>
    internal async Task WaitUntilIdleAsync(CancellationToken cancellationToken)
    {
        if (_sendIdle.Task.IsCompleted)
            return;

        try
        {
            await _sendIdle.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("Outbox processor was still publishing when the host shutdown timed out.");
        }
    }

    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _sendStop.Token);
        var send = linked.Token;
        try
        {
            if (_lifetime is not null)
                await WaitForHostStartAsync(_lifetime, send).ConfigureAwait(false);

            _logger.LogInformation("Outbox processor starting");

            while (!send.IsCancellationRequested)
            {
                var ticket = _newEvents?.TakeTicket();
                try
                {
                    await ProcessBatchAsync(send).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (send.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
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
            _sendStop.Dispose();
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

    /// <summary>
    /// Processes a batch of outbox messages.
    /// A stop requested through <paramref name="cancellationToken"/> finishes the current row
    /// and leaves later rows unprocessed.
    /// </summary>
    internal async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        var messages = await _outboxWriter.GetUnprocessedAsync(_batchSize, cancellationToken).ConfigureAwait(false);

        if (messages.Count == 0)
            return;

        _logger.LogDebug("Processing {Count} outbox messages", messages.Count);

        foreach (var message in messages)
        {
            if (cancellationToken.IsCancellationRequested || _sendStop.IsCancellationRequested)
                return;

            if (message.Attempts >= _maxAttempts)
            {
                _logger.LogWarning(
                    "Outbox message {MessageId} has exceeded max attempts ({MaxAttempts}), dead-lettering",
                    message.Id, _maxAttempts);
                OutboxTelemetry.RecordDeadLettered(message.EventType);
                await _outboxWriter.MarkAsDeadLetteredAsync(message.Id, CancellationToken.None).ConfigureAwait(false);
                continue;
            }

            try
            {
                // The current row runs to completion. Shutdown cancels the next row, not this one.
                await _outboxPublisher.PublishAsync(message, CancellationToken.None).ConfigureAwait(false);
                await _outboxWriter.MarkAsProcessedAsync(message.Id, CancellationToken.None).ConfigureAwait(false);

                _logger.LogDebug("Successfully published outbox message {MessageId}", message.Id);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to publish outbox message {MessageId}", message.Id);
                OutboxTelemetry.RecordPublishFailure(message.EventType);
                await _outboxWriter.RecordFailureAsync(message.Id, ex.Message, CancellationToken.None).ConfigureAwait(false);
                if (message.Attempts + 1 >= _maxAttempts)
                {
                    _logger.LogWarning(
                        "Outbox message {MessageId} reached max attempts ({MaxAttempts}), dead-lettering",
                        message.Id, _maxAttempts);
                    OutboxTelemetry.RecordDeadLettered(message.EventType);
                    await _outboxWriter.MarkAsDeadLetteredAsync(message.Id, CancellationToken.None).ConfigureAwait(false);
                }
            }
        }
    }
}
