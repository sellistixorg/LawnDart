using LawnDart.Demo.Academy.WebApi.Authorization;
using LawnDart.Demo.Academy.WebApi.Domain.Events;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Sdk;

namespace LawnDart.Demo.Academy.WebApi.Projections;

// ── Student Summary ────────────────────────────────────────────────────────────

/// <summary>
/// Read model for a single student: name, email, and enrollment summary.
/// </summary>
public class StudentSummaryView
{
    /// <summary>Gets or sets the student's unique identifier.</summary>
    public Guid StudentId { get; set; }

    /// <summary>Gets or sets the student's full name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Gets or sets the student's email address.</summary>
    public string Email { get; set; } = string.Empty;

    /// <summary>Gets or sets the number of active course enrollments.</summary>
    public int ActiveEnrollments { get; set; }

    /// <summary>Gets or sets the number of cancelled enrollments.</summary>
    public int CancelledEnrollments { get; set; }

    /// <summary>Gets or sets the UTC timestamp of the most recent event.</summary>
    public DateTime LastUpdated { get; set; }
}

/// <summary>
/// Per-student projection that builds a <see cref="StudentSummaryView"/> from student domain events.
/// One view instance is created per student stream (<c>Student:{studentId}</c>).
/// </summary>
/// <remarks>
/// Demonstrates <see cref="TenantScope.TenantScoped"/>: the GET endpoint requires a valid
/// <c>tenant_id</c> JWT claim to construct the view key, preventing cross-tenant access.
/// </remarks>
[SingleStreamProjection("StudentSummary", streamType: "Student", tenantScope: TenantScope.TenantScoped)]
[ProjectionEndpoint(
    route: "/api/views/students/{studentId}",
    requiredPermission: AcademyPermissions.StudentView,
    cacheMaxAgeSeconds: 30)]
public class StudentSummaryProjection : ProjectionBase<StudentSummaryView>
{
    /// <summary>Handles a student registration event.</summary>
    public void Handle(StudentRegistered e)
    {
        State.StudentId = e.StudentId;
        State.Name = e.Name;
        State.Email = e.Email;
        State.LastUpdated = e.Timestamp;
    }

    /// <summary>Handles a student enrollment event.</summary>
    public void Handle(StudentEnrolled e)
    {
        State.ActiveEnrollments++;
        State.LastUpdated = e.Timestamp;
    }

    /// <summary>Handles an enrollment cancellation event.</summary>
    public void Handle(EnrollmentCancelled e)
    {
        State.ActiveEnrollments = Math.Max(0, State.ActiveEnrollments - 1);
        State.CancelledEnrollments++;
        State.LastUpdated = e.Timestamp;
    }
}

// ── Course Section Summary ─────────────────────────────────────────────────────

/// <summary>
/// Read model for a course section: title, capacity, and live seat availability.
/// </summary>
public class CourseSectionSummaryView
{
    /// <summary>Gets or sets the section's unique identifier.</summary>
    public Guid SectionId { get; set; }

    /// <summary>Gets or sets the section title.</summary>
    public string Title { get; set; } = string.Empty;

    /// <summary>Gets or sets the total number of seats.</summary>
    public int TotalSeats { get; set; }

    /// <summary>Gets or sets the number of reserved seats.</summary>
    public int ReservedSeats { get; set; }

    /// <summary>Gets the number of available seats.</summary>
    public int AvailableSeats => Math.Max(0, TotalSeats - ReservedSeats);

    /// <summary>Gets or sets the UTC timestamp of the most recent event.</summary>
    public DateTime LastUpdated { get; set; }
}

/// <summary>
/// Per-section projection that builds a <see cref="CourseSectionSummaryView"/> from section events.
/// One view instance is created per section stream (<c>Section:{sectionId}</c>).
/// </summary>
/// <remarks>
/// Demonstrates <see cref="TenantScope.TenantScoped"/> for section views; both students and
/// instructors can read this view (see <see cref="AcademyPermissions.SectionView"/>).
/// </remarks>
[SingleStreamProjection("CourseSectionSummary", streamType: "Section", tenantScope: TenantScope.TenantScoped)]
[ProjectionEndpoint(
    route: "/api/views/sections/{sectionId}",
    requiredPermission: AcademyPermissions.SectionView,
    cacheMaxAgeSeconds: 15)]
public class CourseSectionSummaryProjection : ProjectionBase<CourseSectionSummaryView>
{
    /// <summary>Handles a section creation event.</summary>
    public void Handle(SectionCreated e)
    {
        State.SectionId = e.SectionId;
        State.Title = e.Title;
        State.TotalSeats = e.TotalSeats;
        State.LastUpdated = e.Timestamp;
    }

    /// <summary>Handles a seat reservation event.</summary>
    public void Handle(SeatReserved e)
    {
        State.ReservedSeats++;
        State.LastUpdated = e.Timestamp;
    }

    /// <summary>Handles a seat release event.</summary>
    public void Handle(SeatReleased e)
    {
        State.ReservedSeats = Math.Max(0, State.ReservedSeats - 1);
        State.LastUpdated = e.Timestamp;
    }
}

// ── Global Enrollment Index ────────────────────────────────────────────────────

/// <summary>
/// System-wide enrollment statistics aggregated across all tenants and students.
/// </summary>
public class EnrollmentIndexView
{
    /// <summary>Gets or sets the total number of students registered across all tenants.</summary>
    public int TotalStudents { get; set; }

    /// <summary>Gets or sets the total number of active enrollments across all tenants.</summary>
    public int TotalActiveEnrollments { get; set; }

    /// <summary>Gets or sets the total number of cancelled enrollments.</summary>
    public int TotalCancelledEnrollments { get; set; }

    /// <summary>Gets or sets the UTC timestamp of the most recent event.</summary>
    public DateTime LastUpdated { get; set; }
}

/// <summary>
/// Global projection that builds an <see cref="EnrollmentIndexView"/> from all enrollment
/// events across the entire event store. Produces a single system-wide view instance.
/// </summary>
/// <remarks>
/// Demonstrates <see cref="TenantScope.SystemGlobal"/>: no tenant claim is required to read
/// this view, but the endpoint still enforces the
/// <see cref="AcademyPermissions.EnrollmentIndexView"/> permission.
/// </remarks>
[GlobalProjection("EnrollmentIndex", tenantScope: TenantScope.SystemGlobal)]
[ProjectionEndpoint(
    route: "/api/views/enrollment-index",
    requiredPermission: AcademyPermissions.EnrollmentIndexView)]
public class GlobalEnrollmentIndexProjection : ProjectionBase<EnrollmentIndexView>
{
    /// <summary>Handles a student registration event (increments total students).</summary>
    public void Handle(StudentRegistered e)
    {
        State.TotalStudents++;
        State.LastUpdated = e.Timestamp;
    }

    /// <summary>Handles a student enrollment event.</summary>
    public void Handle(StudentEnrolled e)
    {
        State.TotalActiveEnrollments++;
        State.LastUpdated = e.Timestamp;
    }

    /// <summary>Handles an enrollment cancellation event.</summary>
    public void Handle(EnrollmentCancelled e)
    {
        State.TotalActiveEnrollments = Math.Max(0, State.TotalActiveEnrollments - 1);
        State.TotalCancelledEnrollments++;
        State.LastUpdated = e.Timestamp;
    }
}
