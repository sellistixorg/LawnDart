using LawnDart;
using LawnDart.Dcb;
using LawnDart.Demo.Shop.Domain.Product.Commands;
using LawnDart.Demo.Shop.Domain.Product.Events;

namespace LawnDart.Demo.Shop.Domain.Product;

/// <summary>
/// DCB entity that enforces per-seller SKU uniqueness.
///
/// When two concurrent <see cref="ReserveSkuCommand"/> requests arrive for the same
/// seller+SKU combination, the DCB <c>AppendCondition</c> ensures only one succeeds :
/// the second receives a <c>ConcurrencyException</c> which the API layer translates to 409.
///
/// The tag <c>sku:{tenantId}:{sku}</c> scopes the consistency boundary to a single
/// seller's SKU namespace, so Bob and Larry can each have their own "SKU-001" without
/// conflict.
/// </summary>
public class SkuRegistryEntity : DcbEntity<SkuRegistryState>
{
    public SkuRegistryEntity() { }

    public static string[] GetSkuTags(string tenantId, string sku)
        => [$"sku:{tenantId}:{sku.ToUpperInvariant()}"];

    public void Handle(ReserveSkuCommand cmd)
    {
        if (State.IsReserved)
            throw new DomainException(
                $"SKU '{cmd.Sku}' is already in use by another product for this seller.");

        Emit(new SkuReserved(Guid.NewGuid(), DateTime.UtcNow, cmd.Sku.ToUpperInvariant(), cmd.ProductId, cmd.TenantId),
            $"sku:{cmd.TenantId}:{cmd.Sku.ToUpperInvariant()}");
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        if (@event is SkuReserved)
            State.IsReserved = true;
    }
}

public class SkuRegistryState : IState
{
    public bool IsReserved { get; set; }
}
