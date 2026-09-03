namespace LawnDart.Demo.Academy.WebApi;

/// <summary>
/// Permission constants for the Academy Web API.
/// Used on command handlers via <c>[RequiresPermission]</c> and on projection endpoints
/// via <see cref="LawnDart.Projections.Lightweight.ProjectionEndpointAttribute"/>.
/// </summary>
public static class AcademyPermissions
{
    /// <summary>Read a student's summary view.</summary>
    public const string StudentView = "Student.View";

    /// <summary>Register a new student or enroll a student in a section.</summary>
    public const string StudentEnroll = "Student.Enroll";

    /// <summary>Read a course section's seat availability view.</summary>
    public const string SectionView = "Section.View";

    /// <summary>Create a new course section.</summary>
    public const string SectionCreate = "Section.Create";

    /// <summary>Read the global enrollment statistics index.</summary>
    public const string EnrollmentIndexView = "EnrollmentIndex.View";
}
