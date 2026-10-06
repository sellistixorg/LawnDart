using LawnDart;
using LawnDart.EventStore;
using Microsoft.Extensions.Logging;

namespace LawnDart.Demo.MultiContextInMemory.Catalog;

/// <summary>Publishes a product on the catalog stream.</summary>
public sealed record PublishProductCommand(Guid Id, string Name, decimal Price) : ICommand;

/// <summary>Retires a published product.</summary>
public sealed record RetireProductCommand(Guid Id) : ICommand;

/// <summary>A product was published.</summary>
[EventTypeName("product-published")]
public record ProductPublished(Guid Id, DateTime Timestamp, string Name, decimal Price) : IEvent;

/// <summary>A product was retired.</summary>
[EventTypeName("product-retired")]
public record ProductRetired(Guid Id, DateTime Timestamp) : IEvent;

/// <summary>
/// Handles <see cref="PublishProductCommand"/>.
/// Injects <see cref="IEventStore"/>. The catalog store is a different instance
/// from the store injected into ordering handlers.
/// </summary>
public sealed class PublishProductHandler : ICommandHandler<PublishProductCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ILogger<PublishProductHandler> _logger;

    /// <summary>Creates the handler.</summary>
    public PublishProductHandler(IEventStore eventStore, ILogger<PublishProductHandler> logger)
    {
        _eventStore = eventStore;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task HandleAsync(PublishProductCommand command, CancellationToken cancellationToken = default)
    {
        var streamId = $"Product:{command.Id}";
        var evt = new ProductPublished(Guid.NewGuid(), DateTime.UtcNow, command.Name, command.Price);

        await _eventStore.AppendAsync(streamId, [evt], cancellationToken: cancellationToken);

        _logger.LogInformation(
            "  [catalog] Product '{Name}' (${Price:F2}) published with id {ProductId}",
            command.Name, command.Price, command.Id);
    }
}

/// <summary>Handles <see cref="RetireProductCommand"/>.</summary>
public sealed class RetireProductHandler : ICommandHandler<RetireProductCommand>
{
    private readonly IEventStore _eventStore;
    private readonly ILogger<RetireProductHandler> _logger;

    /// <summary>Creates the handler.</summary>
    public RetireProductHandler(IEventStore eventStore, ILogger<RetireProductHandler> logger)
    {
        _eventStore = eventStore;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task HandleAsync(RetireProductCommand command, CancellationToken cancellationToken = default)
    {
        var streamId = $"Product:{command.Id}";
        var evt = new ProductRetired(Guid.NewGuid(), DateTime.UtcNow);

        await _eventStore.AppendAsync(streamId, [evt], cancellationToken: cancellationToken);

        _logger.LogInformation("  [catalog] Product {ProductId} retired", command.Id);
    }
}
