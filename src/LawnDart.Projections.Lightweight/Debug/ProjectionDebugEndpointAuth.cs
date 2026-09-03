using Microsoft.AspNetCore.Builder;

namespace LawnDart.Projections.Lightweight.Debug;

/// <summary>
/// Shared authorization helpers for projection debug HTTP endpoints and UX routes.
/// </summary>
public static class ProjectionDebugEndpointAuth
{
    /// <summary>
    /// Applies authentication and optional authorization policies to a route handler.
    /// </summary>
    public static void Apply(RouteHandlerBuilder builder, bool requireAuth, string[] policyNames)
    {
        if (!requireAuth)
            return;

        if (policyNames.Length > 0)
        {
            foreach (var policy in policyNames)
                builder.RequireAuthorization(policy);
        }
        else
        {
            builder.RequireAuthorization();
        }
    }
}
