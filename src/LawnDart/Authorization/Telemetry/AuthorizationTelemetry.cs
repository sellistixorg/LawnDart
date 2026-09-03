using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LawnDart.Authorization.Telemetry;

/// <summary>
/// Telemetry for authorization operations.
/// </summary>
public static class AuthorizationTelemetry
{
    private static readonly ActivitySource ActivitySource = new("LawnDart.Authorization");
    private static readonly Meter Meter = new("LawnDart.Authorization");
    
    private static readonly Histogram<double> CheckDuration = Meter.CreateHistogram<double>(
        "authorization.check.duration",
        "ms",
        "Duration of authorization checks");
    
    private static readonly Counter<long> CheckResult = Meter.CreateCounter<long>(
        "authorization.check.result",
        "count",
        "Authorization check results (success/failure)");
    
    private static readonly Counter<long> PermissionDenied = Meter.CreateCounter<long>(
        "authorization.permission.denied",
        "count",
        "Permission denied events");
    
    private static readonly Counter<long> EntitlementDenied = Meter.CreateCounter<long>(
        "authorization.entitlement.denied",
        "count",
        "Entitlement denied events");
    
    private static readonly Counter<long> ContextMissing = Meter.CreateCounter<long>(
        "authorization.context.missing",
        "count",
        "Missing authorization context events");
    
    /// <summary>
    /// Starts an authorization check activity.
    /// </summary>
    /// <param name="commandType">Type of command being authorized.</param>
    /// <returns>Activity or null if not enabled.</returns>
    public static Activity? StartAuthorizationCheck(string commandType)
    {
        var activity = ActivitySource.StartActivity("AuthorizationCheck");
        activity?.SetTag("command.type", commandType);
        return activity;
    }
    
    /// <summary>
    /// Records the result of an authorization check.
    /// </summary>
    /// <param name="success">Whether the check succeeded.</param>
    /// <param name="duration">Duration of the check.</param>
    public static void RecordCheckResult(bool success, TimeSpan duration)
    {
        CheckDuration.Record(duration.TotalMilliseconds);
        CheckResult.Add(1, new KeyValuePair<string, object?>("result", success ? "success" : "failure"));
    }
    
    /// <summary>
    /// Records a permission denied event.
    /// </summary>
    /// <param name="permission">Permission that was denied.</param>
    public static void RecordPermissionDenied(string permission)
    {
        PermissionDenied.Add(1, new KeyValuePair<string, object?>("permission", permission));
    }
    
    /// <summary>
    /// Records an entitlement denied event.
    /// </summary>
    /// <param name="entitlement">Entitlement that was denied.</param>
    public static void RecordEntitlementDenied(string entitlement)
    {
        EntitlementDenied.Add(1, new KeyValuePair<string, object?>("entitlement", entitlement));
    }
    
    /// <summary>
    /// Records a missing authorization context event.
    /// </summary>
    /// <param name="providerType">Type of provider that couldn't provide context.</param>
    public static void RecordContextMissing(string providerType)
    {
        ContextMissing.Add(1, new KeyValuePair<string, object?>("provider_type", providerType));
    }
}
