namespace LawnDart.Authorization;

/// <summary>
/// Captures authorization context from the current execution context.
/// Different implementations for HTTP, messaging, background services, etc.
/// </summary>
public interface IAuthorizationContextProvider
{
    /// <summary>
    /// Captures the authorization context from the current execution environment.
    /// Returns null if no context is available.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Authorization context or null if not available.</returns>
    Task<AuthorizationContext?> GetAuthorizationContextAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Indicates if this provider can handle the current execution context.
    /// </summary>
    /// <returns>True if this provider can provide context in the current environment.</returns>
    bool CanProvideContext();
}
