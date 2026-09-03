namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Marks a <see cref="LawnDart.Projections.Sdk.ProjectionBase{TView}"/> subclass as a
/// Decision Consistent Boundary (DCB) projection that reads only events matching a specific set of
/// event types from the global sequence, producing a single view instance.
/// </summary>
/// <remarks>
/// <para>
/// A <c>DcbProjection</c> is suitable for cross-stream read models that need only a targeted
/// subset of events. The runner builds a <see cref="LawnDart.EventStore.Query"/> from
/// <see cref="QueryTypes"/> and polls using
/// <c>IEventStore.ReadByQueryStreamAsync(query, fromSequencePosition)</c>.
/// </para>
/// <para>
/// Events whose type is not handled by the projection's <c>Handle</c> methods are silently
/// ignored by <see cref="LawnDart.Projections.Sdk.ProjectionBase{TView}.ProcessEvent"/>.
/// </para>
/// <para>
/// Multi-node deployments use the same per-node view replica model as
/// <see cref="GlobalProjectionAttribute"/> (<c>global</c> vs <c>global:n{NodeInstance}</c>).
/// </para>
/// <example>
/// <code>
/// [DcbProjection("StudentActivityFeed", "StudentRegistered", "StudentEnrolled", "RegistrationCancelled")]
/// public class StudentActivityFeedProjection : ProjectionBase&lt;StudentActivityFeedView&gt;
/// {
///     public void Handle(StudentRegistered e) { State.Activities.Add($"Registered: {e.Name}"); }
///     public void Handle(StudentEnrolled e)   { State.Activities.Add($"Enrolled in course"); }
/// }
/// </code>
/// </example>
/// </remarks>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class DcbProjectionAttribute : ProjectionDefinitionAttribute
{
    /// <summary>
    /// Gets the event type names used to build the DCB query
    /// (e.g., <c>"StudentRegistered"</c>, <c>"StudentEnrolled"</c>).
    /// An empty array results in <see cref="LawnDart.EventStore.Query.All()"/> being used.
    /// </summary>
    public string[] QueryTypes { get; }

    /// <summary>
    /// Initializes a new <see cref="DcbProjectionAttribute"/> with <see cref="TenantScope.SystemGlobal"/>.
    /// </summary>
    /// <param name="name">Unique projection name.</param>
    /// <param name="queryTypes">
    /// One or more event type names to include in the DCB query.
    /// Pass an empty array to match all event types.
    /// </param>
    public DcbProjectionAttribute(string name, params string[] queryTypes)
        : base(name, TenantScope.SystemGlobal)
    {
        QueryTypes = queryTypes ?? Array.Empty<string>();
    }

    /// <summary>
    /// Initializes a new <see cref="DcbProjectionAttribute"/> with an explicit tenant scope.
    /// </summary>
    /// <param name="name">Unique projection name.</param>
    /// <param name="tenantScope">Tenant isolation mode.</param>
    /// <param name="queryTypes">One or more event type names to include in the DCB query.</param>
    public DcbProjectionAttribute(string name, TenantScope tenantScope, params string[] queryTypes)
        : base(name, tenantScope)
    {
        QueryTypes = queryTypes ?? Array.Empty<string>();
    }
}
