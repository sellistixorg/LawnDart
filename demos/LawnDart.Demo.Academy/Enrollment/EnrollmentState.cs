using LawnDart;
using LawnDart.Dcb;
using LawnDart.Demo.Academy.Domain.Student.Events;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;
using LawnDart.Demo.Academy.Domain.Student.Commands;
using LawnDart.Demo.Academy.Domain.CourseSection.Commands;

namespace LawnDart.Demo.Academy.Enrollment;

/// <summary>
/// State for one student's enrollment in one section.
/// Seat counts come from every student's seat events for the section.
/// Enrolled, confirmation, and cancelled flags come from this student's events.
/// </summary>
public class EnrollmentState : IState
{
    public Guid StudentId { get; set; }
    public Guid SectionId { get; set; }
    public Guid CourseId { get; set; }
    public string StudentEmail { get; set; } = string.Empty;
    public bool StudentIsActive { get; set; }
    public int SeatsAvailable { get; set; }
    public bool SeatReserved { get; set; }
    public bool StudentEnrolled { get; set; }
    public bool ConfirmationSent { get; set; }
    public bool IsCancelled { get; set; }

    public bool CanEnroll => StudentIsActive && SeatsAvailable > 0 && !SeatReserved && !StudentEnrolled;
}
