using Microsoft.Extensions.Logging;
using LawnDart.Authorization.Telemetry;

namespace LawnDart.Authorization;

/// <summary>
/// Orchestrates authorization checks by extracting context and invoking the provider.
/// </summary>
public class AuthorizationService
{
    private readonly IAuthorizationContextProvider _contextProvider;
    private readonly IAuthorizationProvider _authorizationProvider;
    private readonly ILogger<AuthorizationService>? _logger;
    
    /// <summary>
    /// Initializes a new instance of the AuthorizationService.
    /// </summary>
    /// <param name="contextProvider">Provider for extracting authorization context.</param>
    /// <param name="authorizationProvider">Provider for performing authorization checks.</param>
    /// <param name="logger">Optional logger.</param>
    public AuthorizationService(
        IAuthorizationContextProvider contextProvider,
        IAuthorizationProvider authorizationProvider,
        ILogger<AuthorizationService>? logger = null)
    {
        _contextProvider = contextProvider ?? throw new ArgumentNullException(nameof(contextProvider));
        _authorizationProvider = authorizationProvider ?? throw new ArgumentNullException(nameof(authorizationProvider));
        _logger = logger;
    }
    
    /// <summary>
    /// Authorizes a command by checking its declarative attributes.
    /// </summary>
    /// <typeparam name="TCommand">Command type.</typeparam>
    /// <param name="command">Command instance.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Authorization result.</returns>
    public async Task<AuthorizationResult> AuthorizeCommandAsync<TCommand>(
        TCommand command,
        CancellationToken cancellationToken = default) where TCommand : ICommand
    {
        using var activity = AuthorizationTelemetry.StartAuthorizationCheck(typeof(TCommand).Name);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Get authorization context from transport
            var context = await _contextProvider.GetAuthorizationContextAsync(cancellationToken);
            if (context == null)
            {
                _logger?.LogWarning("No authorization context available for command {CommandType}", typeof(TCommand).Name);
                AuthorizationTelemetry.RecordContextMissing(_contextProvider.GetType().Name);
                return AuthorizationResult.Failure("No authorization context available");
            }
            
            var commandType = typeof(TCommand);
            var failedChecks = new List<string>();
            
            // Check permissions
            var permissionAttrs = commandType.GetCustomAttributes(typeof(RequiresPermissionAttribute), true)
                .Cast<RequiresPermissionAttribute>();
            
            foreach (var attr in permissionAttrs)
            {
                var result = await _authorizationProvider.CheckPermissionAsync(context, attr.Permission, cancellationToken);
                if (!result.IsAuthorized)
                {
                    failedChecks.Add($"Permission: {attr.Permission}");
                    AuthorizationTelemetry.RecordPermissionDenied(attr.Permission);
                }
            }
            
            // Check entitlements
            var entitlementAttrs = commandType.GetCustomAttributes(typeof(RequiresEntitlementAttribute), true)
                .Cast<RequiresEntitlementAttribute>();
            
            foreach (var attr in entitlementAttrs)
            {
                var result = await _authorizationProvider.CheckEntitlementAsync(context, attr.Entitlement, cancellationToken);
                if (!result.IsAuthorized)
                {
                    failedChecks.Add($"Entitlement: {attr.Entitlement}");
                    AuthorizationTelemetry.RecordEntitlementDenied(attr.Entitlement);
                }
            }
            
            // Check policies
            var policyAttrs = commandType.GetCustomAttributes(typeof(RequiresPolicyAttribute), true)
                .Cast<RequiresPolicyAttribute>();
            
            foreach (var attr in policyAttrs)
            {
                var result = await _authorizationProvider.CheckPolicyAsync(context, attr.PolicyName, cancellationToken);
                if (!result.IsAuthorized)
                {
                    failedChecks.Add($"Policy: {attr.PolicyName}");
                }
            }
            
            if (failedChecks.Count > 0)
            {
                _logger?.LogWarning(
                    "Authorization failed for command {CommandType}. Failed checks: {FailedChecks}",
                    commandType.Name,
                    string.Join(", ", failedChecks));
                
                AuthorizationTelemetry.RecordCheckResult(false, stopwatch.Elapsed);
                return AuthorizationResult.Failure(
                    "Authorization failed",
                    failedChecks.ToArray());
            }
            
            _logger?.LogDebug("Command {CommandType} authorized for user {UserId}", commandType.Name, context.UserId);
            AuthorizationTelemetry.RecordCheckResult(true, stopwatch.Elapsed);
            return AuthorizationResult.Success();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error during authorization check for command {CommandType}", typeof(TCommand).Name);
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
            AuthorizationTelemetry.RecordCheckResult(false, stopwatch.Elapsed);
            throw;
        }
    }
}
