namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Marks a <see cref="LawnDart.Projections.Sdk.ProjectionBase{TView}"/> subclass as a
/// per-stream projection that builds an independent view for each individual stream of a given
/// aggregate type.
/// </summary>
/// <remarks>
/// <para>
/// A <c>SingleStreamProjection</c> produces one view instance per stream.  At startup the runner
/// loads any previously saved view states from <see cref="LawnDart.Projections.Storage.IViewStore"/>
/// and resumes processing from the last global checkpoint, creating new instances lazily as
/// events for previously unseen stream IDs arrive.
/// </para>
/// <para>
/// In multi-node deployments the <see cref="LawnDart.Projections.Partitioning.IPartitioningService"/>
/// ensures each node only processes the streams assigned to it via consistent hashing, preventing
/// duplicate view writes.
/// </para>
/// <example>
/// <code>
/// [SingleStreamProjection("StudentSummary", streamType: "Student")]
/// [ProjectionEndpoint("/api/views/students/{studentId}", requiredPermission: AcademyPermissions.StudentView)]
/// public class StudentSummaryProjection : ProjectionBase&lt;StudentSummaryView&gt;
/// {
///     public void Handle(StudentRegistered e) { State.Name = e.Name; }
///     public void Handle(StudentEnrolled e)   { State.EnrolledCourses++; }
/// }
/// </code>
/// </example>
/// </remarks>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class SingleStreamProjectionAttribute : ProjectionDefinitionAttribute
{
    /// <summary>
    /// Gets the aggregate stream type whose streams this projection handles
    /// (e.g., <c>"Student"</c> matches stream IDs like <c>tenant:Student:guid</c>).
    /// </summary>
    public string StreamType { get; }

    /// <summary>
    /// Initializes a new <see cref="SingleStreamProjectionAttribute"/>.
    /// </summary>
    /// <param name="logicalName">Logical projection family name used for discovery and routing metadata.</param>
    /// <param name="streamType">
    /// Aggregate type to project (e.g., <c>"Student"</c>). Events are filtered to streams
    /// whose stream ID contains this type segment.
    /// </param>
    /// <param name="tenantScope">
    /// Tenant isolation mode. Defaults to <see cref="TenantScope.TenantScoped"/>.
    /// </param>
    public SingleStreamProjectionAttribute(
        string logicalName,
        string streamType,
        TenantScope tenantScope = TenantScope.TenantScoped)
        : base(logicalName, tenantScope)
    {
        StreamType = streamType ?? throw new ArgumentNullException(nameof(streamType));
    }
}
