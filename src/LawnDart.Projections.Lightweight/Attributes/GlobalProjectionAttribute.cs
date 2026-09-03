namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Marks a <see cref="LawnDart.Projections.Sdk.ProjectionBase{TView}"/> subclass as a
/// global projection that reads from the full global event sequence and produces a single view instance.
/// </summary>
/// <remarks>
/// <para>
/// A <c>GlobalProjection</c> is ideal for system-wide indexes, counts, and cross-stream
/// aggregations. It processes all events in global sequence order using
/// <c>IEventStore.ReadByQueryStreamAsync(Query.All(), fromSequencePosition)</c>.
/// </para>
/// <para>
/// In multi-node deployments (<c>TotalInstances &gt; 1</c>), each node builds a <b>full replica</b>
/// of this view under a node-isolated instance id (<c>global:n{NodeInstance}</c>). Checkpoints
/// remain per <c>NodeId</c>. This avoids racing on a shared <c>"global"</c> row.
/// </para>
/// <example>
/// <code>
/// [GlobalProjection("EnrollmentIndex")]
/// [ProjectionEndpoint("/api/views/enrollment-index", requiredPermission: AcademyPermissions.EnrollmentIndexView)]
/// public class GlobalEnrollmentIndexProjection : ProjectionBase&lt;EnrollmentIndexView&gt;
/// {
///     public void Handle(StudentEnrolled e) { State.TotalEnrollments++; }
/// }
/// </code>
/// </example>
/// </remarks>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class GlobalProjectionAttribute : ProjectionDefinitionAttribute
{
    /// <summary>
    /// Initializes a new <see cref="GlobalProjectionAttribute"/>.
    /// </summary>
    /// <param name="name">Unique projection name.</param>
    /// <param name="tenantScope">
    /// Tenant isolation mode. Defaults to <see cref="TenantScope.SystemGlobal"/>.
    /// </param>
    public GlobalProjectionAttribute(
        string name,
        TenantScope tenantScope = TenantScope.SystemGlobal)
        : base(name, tenantScope)
    {
    }
}
