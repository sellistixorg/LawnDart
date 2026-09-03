namespace LawnDart.Metadata;

/// <summary>
/// An <see cref="ITenantContextProvider"/> that tries a prioritised list of inner providers
/// in order, returning the first non-null tenant ID.
/// </summary>
/// <remarks>
/// <para>
/// Typical composition in a SaaS application:
/// <list type="number">
///   <item><description><c>JwtTenantContextProvider</c> — resolves from JWT claim for HTTP requests.</description></item>
///   <item><description><c>AmbientTenantContextProvider</c> — resolves from <see cref="AsyncLocal{T}"/> for background jobs.</description></item>
///   <item><description><c>FixedTenantContextProvider("shop")</c> — fallback for dedicated platform service calls.</description></item>
/// </list>
/// </para>
/// <para>
/// The JWT provider extension method (<c>AddJwtTenantContextProvider()</c>) is defined in
/// <c>LawnDart.AspNetCore</c> or <c>LawnDart.Projections.Lightweight</c>,
/// not in Core, to avoid a circular dependency.
/// </para>
/// </remarks>
public sealed class CompositeTenantContextProvider : ITenantContextProvider
{
    private readonly IReadOnlyList<ITenantContextProvider> _providers;

    /// <param name="providers">Ordered list of providers to try. Must not be empty.</param>
    public CompositeTenantContextProvider(IReadOnlyList<ITenantContextProvider> providers)
    {
        ArgumentNullException.ThrowIfNull(providers);
        if (providers.Count == 0)
            throw new ArgumentException("At least one ITenantContextProvider is required.", nameof(providers));
        _providers = providers;
    }

    /// <inheritdoc />
    public string? GetTenantId()
    {
        foreach (var provider in _providers)
        {
            var tenantId = provider.GetTenantId();
            if (!string.IsNullOrWhiteSpace(tenantId))
                return tenantId;
        }
        return null;
    }

    /// <inheritdoc />
    public string GetTenantIdRequired()
    {
        var tenantId = GetTenantId();
        if (!string.IsNullOrWhiteSpace(tenantId))
            return tenantId;

        var providerNames = string.Join(", ", _providers.Select(p => p.GetType().Name));
        throw new InvalidOperationException(
            $"Tenant ID could not be resolved. All providers returned null: [{providerNames}]. " +
            "Ensure a valid ITenantContextProvider is configured for the current execution context.");
    }
}
