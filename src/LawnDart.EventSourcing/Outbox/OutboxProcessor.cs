using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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
    
    /// <summary>
    /// Initializes a new instance of the OutboxProcessor.
    /// </summary>
    /// <param name="outboxWriter">Outbox writer for reading and updating messages.</param>
    /// <param name="outboxPublisher">Publisher for sending messages to external systems.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="pollingInterval">How often to poll for new messages. Default is 5 seconds.</param>
    /// <param name="batchSize">Number of messages to process per batch. Default is 100.</param>
    /// <param name="maxAttempts">Maximum retry attempts before giving up. Default is 10.</param>
    public OutboxProcessor(
        IOutboxWriter outboxWriter,
        IOutboxPublisher outboxPublisher,
        ILogger<OutboxProcessor> logger,
        TimeSpan? pollingInterval = null,
        int batchSize = 100,
        int maxAttempts = 10)
    {
        _outboxWriter = outboxWriter ?? throw new ArgumentNullException(nameof(outboxWriter));
        _outboxPublisher = outboxPublisher ?? throw new ArgumentNullException(nameof(outboxPublisher));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _pollingInterval = pollingInterval ?? TimeSpan.FromSeconds(5);
        _batchSize = batchSize;
        _maxAttempts = maxAttempts;
    }
    
    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox processor starting");
        
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessBatchAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing outbox batch");
            }
            
            await Task.Delay(_pollingInterval, stoppingToken);
        }
        
        _logger.LogInformation("Outbox processor stopping");
    }
    
    /// <summary>
    /// Processes a batch of outbox messages.
    /// </summary>
    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        var messages = await _outboxWriter.GetUnprocessedAsync(_batchSize, cancellationToken);
        
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
                await _outboxWriter.MarkAsDeadLetteredAsync(message.Id, cancellationToken);
                continue;
            }
            
            try
            {
                await _outboxPublisher.PublishAsync(message, cancellationToken);
                await _outboxWriter.MarkAsProcessedAsync(message.Id, cancellationToken);
                
                _logger.LogDebug("Successfully published outbox message {MessageId}", message.Id);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to publish outbox message {MessageId}", message.Id);
                await _outboxWriter.RecordFailureAsync(message.Id, ex.Message, cancellationToken);
                if (message.Attempts + 1 >= _maxAttempts)
                {
                    _logger.LogWarning(
                        "Outbox message {MessageId} reached max attempts ({MaxAttempts}), dead-lettering",
                        message.Id, _maxAttempts);
                    await _outboxWriter.MarkAsDeadLetteredAsync(message.Id, cancellationToken);
                }
            }
        }
    }
}
