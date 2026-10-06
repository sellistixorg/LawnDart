using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Authorization;
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

/// <summary>
/// Buyer-tenant stream id for an order: <c>{buyerTenant}:OrderAggregate:{id}</c>.
/// </summary>
internal static class OrderStreams
{
    public static string Id(Guid orderId, string buyerTenant)
        => $"{buyerTenant}:OrderAggregate:{orderId}";

    public static Task<OrderAggregate> LoadAsync(
        IAggregateRepository repository,
        Guid orderId,
        string buyerTenant,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(buyerTenant))
            return repository.GetOrCreateAsync<OrderAggregate>(orderId, cancellationToken);

        return repository.GetOrCreateAsync<OrderAggregate>(Id(orderId, buyerTenant), cancellationToken);
    }
}

/// <summary>
/// Creates a product, reserves its SKU, and opens the inventory stream.
/// </summary>
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

/// <summary>
/// Applies a stock delta on <see cref="InventoryAggregate"/>.
/// </summary>
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

/// <summary>
/// Updates the price on <see cref="ProductAggregate"/>.
/// </summary>
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
/// Places an order. If that order id already exists, this fails before stock is reserved.
/// Stock reservation and the order append are two writes.
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
        var meta = _metadata.CaptureCommandMetadata();
        var buyerTenant = meta.TenantId ?? command.TenantId;
        var order = await OrderStreams.LoadAsync(_repo, command.OrderId, buyerTenant, cancellationToken)
            .ConfigureAwait(false);
        if (order.State.Exists)
            throw new DomainException($"Order {command.OrderId} already exists.");

        // Load with the product tag so the DCB query finds product and stock events.
        var tags = InventoryEntity.GetProductTags(command.ProductId);
        var entity = await _dcb.GetOrCreateEntityAsync<InventoryEntity>(tags, cancellationToken)
            .ConfigureAwait(false);
        await _dcb.HandleCommandAsync<InventoryEntity, PlaceOrderCommand>(entity, command, meta, cancellationToken)
            .ConfigureAwait(false);

        var sellerFulfillmentId = command.SellerFulfillmentId == default
            ? Guid.NewGuid()
            : command.SellerFulfillmentId;

        var enriched = command with
        {
            CustomerId          = meta.UserId   ?? command.CustomerId,
            CustomerName        = meta.UserName ?? command.CustomerName,
            SellerFulfillmentId = sellerFulfillmentId,
            TenantId            = buyerTenant
        };
        await _repo.HandleCommandAsync(order, enriched, meta, cancellationToken).ConfigureAwait(false);

        var orderPlaced = new OrderPlaced(Guid.NewGuid(), DateTime.UtcNow,
            command.OrderId, enriched.CustomerId, enriched.CustomerName,
            command.ProductId, command.ProductName, command.Quantity, command.UnitPrice,
            command.SellerId, command.SellerName, sellerFulfillmentId, enriched.TenantId);

        await ShopMessages.PublishAsync(_transport, orderPlaced, meta, _runtime.PublishDirectly, cancellationToken)
            .ConfigureAwait(false);
    }
}

/// <summary>
/// Records payment and publishes <see cref="OrderPaymentProcessed"/> when direct publish is on.
/// </summary>
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

/// <summary>
/// Ships an order on the buyer-tenant stream named by <see cref="ShipOrderCommand.TenantId"/>.
/// </summary>
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
        var meta = _metadata.CaptureCommandMetadata();
        var order = await OrderStreams.LoadAsync(_repo, command.OrderId, command.TenantId, cancellationToken)
            .ConfigureAwait(false);
        await _repo.HandleCommandAsync(order, command, meta, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
/// Cancels an order on the buyer-tenant stream. A signed-in seller can cancel only their own orders.
/// </summary>
public sealed class CancelOrderCommandHandler : ICommandHandler<CancelOrderCommand>
{
    private readonly IAggregateRepository _repo;
    private readonly IMetadataProvider _metadata;
    private readonly IAuthorizationContextProvider _authorization;

    public CancelOrderCommandHandler(
        IAggregateRepository repo,
        IMetadataProvider metadata,
        IAuthorizationContextProvider authorization)
    {
        _repo = repo;
        _metadata = metadata;
        _authorization = authorization;
    }

    public async Task HandleAsync(CancelOrderCommand command, CancellationToken cancellationToken = default)
    {
        var meta = _metadata.CaptureCommandMetadata();
        var order = await OrderStreams.LoadAsync(_repo, command.OrderId, command.TenantId, cancellationToken)
            .ConfigureAwait(false);
        await RejectOtherSellersAsync(order, meta, cancellationToken).ConfigureAwait(false);
        await _repo.HandleCommandAsync(order, command, meta, cancellationToken).ConfigureAwait(false);
    }

    private async Task RejectOtherSellersAsync(
        OrderAggregate order,
        CommandMetadata meta,
        CancellationToken cancellationToken)
    {
        if (!order.State.Exists)
            return;

        var caller = await _authorization.GetAuthorizationContextAsync(cancellationToken).ConfigureAwait(false);
        if (caller is null)
            return;

        var isSeller = caller.UserRoles.Contains("Seller", StringComparer.OrdinalIgnoreCase);
        var isBackground = string.Equals(caller.UserName, "system", StringComparison.OrdinalIgnoreCase);
        if (!isSeller || isBackground)
            return;

        if (!string.Equals(order.State.SellerId, meta.TenantId, StringComparison.OrdinalIgnoreCase))
        {
            throw new DomainException(
                $"Order {order.State.OrderId} belongs to seller '{order.State.SellerId}'.");
        }
    }
}
