namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Defines how a projection's tenant identity is resolved when constructing view instance keys
/// and enforcing access control on generated GET endpoints.
/// </summary>
public enum TenantScope
{
    /// <summary>
    /// The view is scoped to the authenticated user's tenant.
    /// The tenant ID is extracted from the JWT claim (<c>tenant_id</c> or <c>tid</c>) and
    /// prepended to the view instance key as <c>{tenantId}:{streamType}:{entityId}</c>.
    /// A <c>401 Unauthorized</c> is returned when the tenant claim is absent.
    /// </summary>
    TenantScoped,

    /// <summary>
    /// The view spans all entities within a tenant but does not enforce cross-tenant isolation.
    /// The instance key is the entity identifier without a tenant prefix.
    /// Suitable for within-tenant aggregations accessible to any authenticated user.
    /// </summary>
    TenantGlobal,

    /// <summary>
    /// The view is system-wide and not tenant-restricted.
    /// No tenant claim is required or validated. Suitable for system-level indexes
    /// and read models that aggregate data across all tenants.
    /// </summary>
    SystemGlobal
}
