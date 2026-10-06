using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.Demo.Shop.Domain.Order;
using LawnDart.Demo.Shop.Domain.Order.Commands;
using LawnDart.Demo.Shop.Domain.Order.Events;
using LawnDart.Demo.Shop.Domain.Product;
using LawnDart.Demo.Shop.Domain.Product.Commands;
using LawnDart.Demo.Shop.Infrastructure;
using LawnDart.Demo.Shop.Inventory;
using LawnDart.Messaging;
using LawnDart.Metadata;
namespace LawnDart.Demo.Shop.Handlers;

// ── Product Handlers ─────────────────────────────────────────────────────────

public sealed class CreateProductCommandHandler : ICommandHandler<CreateProductCommand>
{
    private readonly IDcbRepository _dcb;
    private readonly IAggregateRepository _repo;
    private readonly IMetadataProvider _metadata;

    public CreateProductCommandHandler(IDcbRepository dcb, IAggregateRepository repo, IMetadataProvider metadata)
    {
        _dcb      = dcb;
        _repo     = repo;
        _metadata = metadata;
    }

    public async Task HandleAsync(CreateProductCommand command, CancellationToken cancellationToken = default)
    {
        var meta   = _metadata.CaptureCommandMetadata();
        var tenantId = meta.TenantId ?? string.Empty;

        // Reserve the SKU via DCB so two creates of the same seller SKU cannot both commit.
        //    Two concurrent requests for the same seller+SKU will race here; only one wins.
        var skuTags  = SkuRegistryEntity.GetSkuTags(tenantId, command.Sku);
        var skuEntity = await _dcb.GetOrCreateEntityAsync<SkuRegistryEntity>(skuTags, cancellationToken);
        await _dcb.HandleCommandAsync<SkuRegistryEntity, ReserveSkuCommand>(
            skuEntity,
            new ReserveSkuCommand(Guid.NewGuid(), command.Sku, command.ProductId, tenantId),
            meta,
            cancellationToken);

        // 2. Create the product catalogue entry
        var product = await _repo.GetOrCreateAsync<ProductAggregate>(command.ProductId, cancellationToken);
        await _repo.HandleCommandAsync(product, command, meta, cancellationToken);

        // 3. Initialise the separate inventory aggregate for this product
        var inventory = await _repo.GetOrCreateAsync<InventoryAggregate>(command.ProductId, cancellationToken);
        await _repo.HandleCommandAsync(inventory,
            new InitializeInventoryCommand(Guid.NewGuid(), command.ProductId, command.InitialStock),
            meta, cancellationToken);
    }
}

public sealed class UpdateStockCommandHandler : ICommandHandler<UpdateStockCommand>
{
    private readonly IAggregateRepository _repo;
    private readonly IMetadataProvider _metadata;

    public UpdateStockCommandHandler(IAggregateRepository repo, IMetadataProvider metadata)
    {
        _repo = repo;
        _metadata = metadata;
    }

    public async Task HandleAsync(UpdateStockCommand command, CancellationToken cancellationToken = default)
    {
        var meta      = _metadata.CaptureCommandMetadata();
        // Stock is owned by InventoryAggregate, not ProductAggregate
        var inventory = await _repo.GetOrCreateAsync<InventoryAggregate>(command.ProductId, cancellationToken);
        await _repo.HandleCommandAsync(inventory, command, meta, cancellationToken);
    }
}

public sealed class UpdatePriceCommandHandler : ICommandHandler<UpdatePriceCommand>
{
    private readonly IAggregateRepository _repo;
    private readonly IMetadataProvider _metadata;

    public UpdatePriceCommandHandler(IAggregateRepository repo, IMetadataProvider metadata)
    {
        _repo = repo;
        _metadata = metadata;
    }

    public async Task HandleAsync(UpdatePriceCommand command, CancellationToken cancellationToken = default)
    {
        var meta    = _metadata.CaptureCommandMetadata();
        var product = await _repo.GetOrCreateAsync<ProductAggregate>(command.ProductId, cancellationToken);
        await _repo.HandleCommandAsync(product, command, meta, cancellationToken);
    }
}

// ── Order Handlers ────────────────────────────────────────────────────────────

/// <summary>
/// Places an order using the DCB InventoryEntity to atomically reserve stock and create the order.
/// </summary>
public sealed class PlaceOrderCommandHandler : ICommandHandler<PlaceOrderCommand>
{
    private readonly IDcbRepository _dcb;
    private readonly IAggregateRepository _repo;
    private readonly IMessageTransport _transport;
    private readonly IMetadataProvider _metadata;
    private readonly ShopRuntimeOptions _runtime;

    public PlaceOrderCommandHandler(
        IDcbRepository dcb,
        IAggregateRepository repo,
        IMessageTransport transport,
        IMetadataProvider metadata,
        ShopRuntimeOptions runtime)
    {
        _dcb       = dcb;
        _repo      = repo;
        _transport = transport;
        _metadata  = metadata;
        _runtime   = runtime;
    }

    public async Task HandleAsync(PlaceOrderCommand command, CancellationToken cancellationToken = default)
    {
        var meta   = _metadata.CaptureCommandMetadata();
        // Load entity with product-only tag so the DCB query (AND semantics) finds
        // ProductCreated/StockUpdated events that only carry the product tag.
        // The order tag is added to emitted events inside the entity for traceability.
        var tags   = InventoryEntity.GetProductTags(command.ProductId);
        var entity = await _dcb.GetOrCreateEntityAsync<InventoryEntity>(tags, cancellationToken);
        await _dcb.HandleCommandAsync<InventoryEntity, PlaceOrderCommand>(entity, command, meta, cancellationToken);

        // Generate the seller's unique reference for this fulfillment.
        var sellerFulfillmentId = command.SellerFulfillmentId == default
            ? Guid.NewGuid()
            : command.SellerFulfillmentId;

        // Create the OrderAggregate stream so the payment/ship/cancel lifecycle works.
        // Enrich the command with the authenticated user so the event carries customer info.
        var enriched = command with
        {
            CustomerId          = meta.UserId   ?? command.CustomerId,
            CustomerName        = meta.UserName ?? command.CustomerName,
            SellerFulfillmentId = sellerFulfillmentId,
            TenantId            = meta.TenantId ?? command.TenantId
        };
        var order = await _repo.GetOrCreateAsync<OrderAggregate>(command.OrderId, cancellationToken);
        await _repo.HandleCommandAsync(order, enriched, meta, cancellationToken);

        var orderPlaced = new OrderPlaced(Guid.NewGuid(), DateTime.UtcNow,
            command.OrderId, enriched.CustomerId, enriched.CustomerName,
            command.ProductId, command.ProductName, command.Quantity, command.UnitPrice,
            command.SellerId, command.SellerName, sellerFulfillmentId, enriched.TenantId);

        await ShopMessages.PublishAsync(_transport, orderPlaced, meta, _runtime.PublishDirectly, cancellationToken)
            .ConfigureAwait(false);
    }
}

public sealed class ProcessPaymentCommandHandler : ICommandHandler<ProcessPaymentCommand>
{
    private readonly IAggregateRepository _repo;
    private readonly IMessageTransport _transport;
    private readonly IMetadataProvider _metadata;
    private readonly ShopRuntimeOptions _runtime;

    public ProcessPaymentCommandHandler(
        IAggregateRepository repo,
        IMessageTransport transport,
        IMetadataProvider metadata,
        ShopRuntimeOptions runtime)
    {
        _repo      = repo;
        _transport = transport;
        _metadata  = metadata;
        _runtime   = runtime;
    }

    public async Task HandleAsync(ProcessPaymentCommand command, CancellationToken cancellationToken = default)
    {
        var meta  = _metadata.CaptureCommandMetadata();
        var order = await _repo.GetOrCreateAsync<OrderAggregate>(command.OrderId, cancellationToken);
        await _repo.HandleCommandAsync(order, command, meta, cancellationToken);

        var evt = new OrderPaymentProcessed(Guid.NewGuid(), DateTime.UtcNow,
            command.OrderId, command.PaymentReference);

        await ShopMessages.PublishAsync(_transport, evt, meta, _runtime.PublishDirectly, cancellationToken)
            .ConfigureAwait(false);
    }
}

public sealed class ShipOrderCommandHandler : ICommandHandler<ShipOrderCommand>
{
    private readonly IAggregateRepository _repo;
    private readonly IMetadataProvider _metadata;

    public ShipOrderCommandHandler(IAggregateRepository repo, IMetadataProvider metadata)
    {
        _repo     = repo;
        _metadata = metadata;
    }

    public async Task HandleAsync(ShipOrderCommand command, CancellationToken cancellationToken = default)
    {
        var meta  = _metadata.CaptureCommandMetadata();
        var order = await _repo.GetOrCreateAsync<OrderAggregate>(command.OrderId, cancellationToken);
        await _repo.HandleCommandAsync(order, command, meta, cancellationToken);
    }
}

public sealed class CancelOrderCommandHandler : ICommandHandler<CancelOrderCommand>
{
    private readonly IAggregateRepository _repo;
    private readonly IMetadataProvider _metadata;

    public CancelOrderCommandHandler(IAggregateRepository repo, IMetadataProvider metadata)
    {
        _repo     = repo;
        _metadata = metadata;
    }

    public async Task HandleAsync(CancelOrderCommand command, CancellationToken cancellationToken = default)
    {
        var meta = _metadata.CaptureCommandMetadata();
        var order = string.IsNullOrEmpty(command.TenantId)
            ? await _repo.GetOrCreateAsync<OrderAggregate>(command.OrderId, cancellationToken).ConfigureAwait(false)
            : await _repo.GetOrCreateAsync<OrderAggregate>(
                $"{command.TenantId}:OrderAggregate:{command.OrderId}",
                cancellationToken).ConfigureAwait(false);
        await _repo.HandleCommandAsync(order, command, meta, cancellationToken).ConfigureAwait(false);
    }
}
