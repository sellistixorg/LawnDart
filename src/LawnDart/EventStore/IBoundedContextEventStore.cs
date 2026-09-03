namespace LawnDart.EventStore;

/// <summary>
/// A named handle to a bounded context's event store stack.
/// </summary>
/// <remarks>
/// <para>
/// Registered as <c>IEnumerable&lt;IBoundedContextEventStore&gt;</c> so tooling (health checks,
/// admin APIs, diagnostics) can enumerate every context registered in the application.
/// </para>
/// <para>
/// This interface is <em>not</em> the primary mechanism for resolving context-specific services
/// in application code — keyed DI (<c>GetRequiredKeyedService&lt;IEventStore&gt;(contextName)</c>)
/// is the runtime binding mechanism.  This interface exists purely for discovery and tooling.
/// </para>
/// </remarks>
public interface IBoundedContextEventStore
{
    /// <summary>
    /// The unique name of this bounded context (e.g. <c>"ordering"</c>, <c>"catalog"</c>).
    /// Used as the DI key for all context-scoped services.
    /// </summary>
    string ContextName { get; }

    /// <summary>The event store instance for this context.</summary>
    IEventStore EventStore { get; }
}
