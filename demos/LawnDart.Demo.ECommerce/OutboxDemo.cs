using Microsoft.Extensions.Logging;
using LawnDart.Aggregates;
using LawnDart.Demo.ECommerce.Domain.Order;
using LawnDart.Demo.ECommerce.Domain.Order.Commands;
using LawnDart.Metadata;
using LawnDart.Outbox;

namespace LawnDart.Demo.ECommerce;

/// <summary>
/// SQL Server transactional outbox. The order event and the outbox row commit
/// together. A dead-letter reset returns that row to the processor, which
/// republishes it with the same message id.
/// </summary>
public class OutboxDemo
{
    private readonly IAggregateRepository _repository;
    private readonly IMetadataProvider _metadataProvider;
    private readonly IOutboxWriter _outboxWriter;
    private readonly ConsoleOutboxPublisher _publisher;
    private readonly ILogger<OutboxDemo> _logger;

    public OutboxDemo(
        IAggregateRepository repository,
        IMetadataProvider metadataProvider,
        IOutboxWriter outboxWriter,
        ConsoleOutboxPublisher publisher,
        ILogger<OutboxDemo> logger)
    {
        _repository = repository;
        _metadataProvider = metadataProvider;
        _outboxWriter = outboxWriter;
        _publisher = publisher;
        _logger = logger;
    }

    public async Task RunAsync(Func<Task> startProcessor)
    {
        Console.WriteLine();
        Console.WriteLine("=== OUTBOX ===");
        Console.WriteLine("The order event and its outbox row commit in one SQL transaction.");
        Console.WriteLine("The processor publishes the row. A reset puts a dead-lettered row back on that queue.");
        Console.WriteLine();

        var before = await _outboxWriter.GetUnprocessedAsync(100);
        var orderId = Guid.NewGuid();
        var order = await _repository.GetOrCreateAsync<Order>(orderId);
        var metadata = _metadataProvider.CaptureCommandMetadata();
        metadata.UserId = "customer-123";

        await _repository.HandleCommandAsync(
            order,
            new CreateOrderCommand(Guid.NewGuid(), orderId, Guid.NewGuid(), "customer-123"),
            metadata);

        var pending = await _outboxWriter.GetUnprocessedAsync(100);
        var created = pending.Where(m => before.All(b => b.Id != m.Id)).ToList();
        if (created.Count == 0)
        {
            throw new InvalidOperationException(
                "CreateOrder wrote no outbox row. EnableOutbox must be on, and the outbox schema must exist.");
        }

        var message = created[0];
        Console.WriteLine($"Order {orderId} on stream {order.StreamId}");
        Console.WriteLine($"Outbox row {message.Id} token {message.EventType} stream {message.StreamId}");

        await _outboxWriter.MarkAsDeadLetteredAsync(message.Id);
        var whileDead = await _outboxWriter.GetUnprocessedAsync(100);
        if (whileDead.Any(m => m.Id == message.Id))
            throw new InvalidOperationException("Dead-lettered row was still in the publish queue.");

        var reset = await _outboxWriter.ResetDeadLetteredAsync(message.Id);
        if (!reset)
            throw new InvalidOperationException("ResetDeadLetteredAsync did not return the row to the queue.");

        var restored = (await _outboxWriter.GetUnprocessedAsync(100)).FirstOrDefault(m => m.Id == message.Id);
        if (restored is null || restored.Attempts != 0)
            throw new InvalidOperationException("Reset row was missing or still had attempts.");

        Console.WriteLine($"Reset {message.Id}. Attempts is 0. Starting the processor.");
        await startProcessor();

        var published = await WaitForPublishAsync(message.Id);
        if (!published)
            throw new InvalidOperationException($"Processor did not publish {message.Id}.");

        var stillPending = await _outboxWriter.GetUnprocessedAsync(100);
        if (stillPending.Any(m => m.Id == message.Id))
            throw new InvalidOperationException("Published row is still unprocessed.");

        Console.WriteLine($"Published {message.Id}. The row is no longer pending.");
        _logger.LogInformation("Outbox demo published {MessageId}", message.Id);
    }

    private async Task<bool> WaitForPublishAsync(Guid messageId)
    {
        for (var i = 0; i < 40; i++)
        {
            if (_publisher.PublishedIds.Contains(messageId))
                return true;
            await Task.Delay(500);
        }

        return false;
    }
}

/// <summary>
/// Prints each outbox row the processor hands it.
/// </summary>
public class ConsoleOutboxPublisher : IOutboxPublisher
{
    private readonly ILogger<ConsoleOutboxPublisher> _logger;
    private readonly List<Guid> _publishedIds = [];

    public ConsoleOutboxPublisher(ILogger<ConsoleOutboxPublisher> logger)
    {
        _logger = logger;
    }

    public IReadOnlyList<Guid> PublishedIds => _publishedIds;

    public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        _publishedIds.Add(message.Id);
        Console.WriteLine($"Published outbox row {message.Id} token {message.EventType} stream {message.StreamId}");
        _logger.LogInformation("Published outbox message {MessageId}", message.Id);
        return Task.CompletedTask;
    }
}
