namespace LawnDart;

/// <summary>
/// Controls how tenants share a bounded context's event store.
/// </summary>
public enum Tenancy
{
    /// <summary>
    /// The store holds a single tenant, named by <c>SingleTenantId</c> or left null for no tenant.
    /// Foreign tenant prefixes on stream ids are rejected.
    /// </summary>
    Single,

    /// <summary>
    /// Many tenants share the same database tables. Every append requires a tenant id,
    /// and queries/conditions are scoped per tenant unless <c>Query.ForAllTenants</c> is used.
    /// </summary>
    Shared,

    /// <summary>
    /// Database-per-tenant isolation. Not supported in this release; refused at startup.
    /// </summary>
    Isolated
}

internal static class TenancyValidation
{
    internal static void EnsureValid(Tenancy tenancy, string? singleTenantId, bool validateNames)
    {
        if (tenancy == Tenancy.Isolated)
        {
            throw new NotSupportedException(
                "Tenancy.Isolated is not supported in this release. Use Tenancy.Single or Tenancy.Shared.");
        }

        if (tenancy is not (Tenancy.Single or Tenancy.Shared))
        {
            throw new ArgumentOutOfRangeException(
                nameof(tenancy),
                tenancy,
                "Tenancy must be Tenancy.Single, Tenancy.Shared, or Tenancy.Isolated.");
        }

        if (tenancy == Tenancy.Shared && singleTenantId is not null)
        {
            throw new ArgumentException(
                "SingleTenantId can only be set when Tenancy is Tenancy.Single.",
                nameof(singleTenantId));
        }

        if (tenancy == Tenancy.Single && singleTenantId is not null)
        {
            LawnDartNames.ValidateTenantId(singleTenantId, nameof(singleTenantId), validateNames);
        }
    }
}
