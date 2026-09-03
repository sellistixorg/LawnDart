namespace LawnDart.Metadata;

/// <summary>
/// An <see cref="ITenantContextProvider"/> that always returns a constant, configuration-supplied
/// tenant ID regardless of the current execution context.
/// </summary>
/// <remarks>
/// <para>
/// Use for services that exclusively operate as a single tenant — for example, a dedicated
/// platform service that always acts as the <c>shop</c> tenant. The tenant identity is
/// determined at deployment configuration time rather than at request time.
/// </para>
/// <para>
/// This provider never returns null and never throws from
/// <see cref="GetTenantIdRequired"/>. It is also safe as the last element in a
/// <see cref="CompositeTenantContextProvider"/> chain to guarantee that a tenant ID
/// is always available.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// // Dedicated platform service — always operates as "shop"
/// services.AddSingleton&lt;ITenantContextProvider&gt;(new FixedTenantContextProvider("shop"));
///
/// // As the final fallback in a composite chain
/// services.AddCompositeTenantContextProvider(p => p
///     .Add(ambientProvider)
///     .Add(new FixedTenantContextProvider("shop")));
/// </code>
/// </example>
public sealed class FixedTenantContextProvider : ITenantContextProvider
{
    private readonly string _tenantId;

    /// <param name="tenantId">The tenant ID that this provider always returns. Must not be null or whitespace.</param>
    public FixedTenantContextProvider(string tenantId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        _tenantId = tenantId;
    }

    /// <inheritdoc />
    public string? GetTenantId() => _tenantId;

    /// <inheritdoc />
    public string GetTenantIdRequired() => _tenantId;
}
