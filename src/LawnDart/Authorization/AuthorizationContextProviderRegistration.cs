namespace LawnDart.Authorization;

/// <summary>
/// DI registration wrapper for leaf <see cref="IAuthorizationContextProvider"/> implementations.
/// Used by <c>AddCompositeAuthorizationContext</c> so the composite can discover providers
/// without calling <c>GetServices&lt;IAuthorizationContextProvider&gt;()</c> (which would
/// recurse into the composite registration itself).
/// </summary>
public class AuthorizationContextProviderRegistration
{
    /// <summary>
    /// Initializes a new registration wrapping <paramref name="provider"/>.
    /// </summary>
    public AuthorizationContextProviderRegistration(IAuthorizationContextProvider provider)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
    }

    /// <summary>
    /// The leaf authorization context provider.
    /// </summary>
    public IAuthorizationContextProvider Provider { get; }
}
