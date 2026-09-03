namespace LawnDart.Authorization;

/// <summary>
/// Tries multiple context providers in order until one succeeds.
/// Enables automatic detection of HTTP vs Message Queue vs Background Service.
/// </summary>
public class CompositeAuthorizationContextProvider : IAuthorizationContextProvider
{
    private readonly IEnumerable<IAuthorizationContextProvider> _providers;
    
    /// <summary>
    /// Initializes a new instance of the CompositeAuthorizationContextProvider.
    /// </summary>
    /// <param name="providers">Collection of context providers to try.</param>
    public CompositeAuthorizationContextProvider(IEnumerable<IAuthorizationContextProvider> providers)
    {
        _providers = providers ?? throw new ArgumentNullException(nameof(providers));
    }
    
    /// <summary>
    /// Returns true if any provider can provide context.
    /// </summary>
    public bool CanProvideContext()
    {
        return _providers.Any(p => p.CanProvideContext());
    }
    
    /// <summary>
    /// Tries each provider in order until one returns a context.
    /// </summary>
    public async Task<AuthorizationContext?> GetAuthorizationContextAsync(CancellationToken cancellationToken = default)
    {
        foreach (var provider in _providers)
        {
            if (provider.CanProvideContext())
            {
                var context = await provider.GetAuthorizationContextAsync(cancellationToken);
                if (context != null)
                    return context;
            }
        }
        
        return null;
    }
}
