namespace LawnDart.Metadata;

/// <summary>
/// Provides tenant context for the current execution context.
/// </summary>
public interface ITenantContextProvider
{
    /// <summary>
    /// Gets the tenant ID from the current execution context.
    /// </summary>
    /// <returns>The tenant ID, or null if not available.</returns>
    string? GetTenantId();

    /// <summary>
    /// Gets the tenant ID from the current execution context, throwing if not available.
    /// </summary>
    /// <returns>The tenant ID.</returns>
    /// <exception cref="InvalidOperationException">Thrown if tenant ID is not available.</exception>
    string GetTenantIdRequired();
}

