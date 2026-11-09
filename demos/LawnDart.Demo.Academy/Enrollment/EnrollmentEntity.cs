using LawnDart;
using LawnDart.Dcb;
using LawnDart.Demo.Academy.Domain.Student.Commands;
using LawnDart.Demo.Academy.Domain.Student.Events;
using LawnDart.Demo.Academy.Domain.CourseSection.Commands;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;
using LawnDart.EventStore;

namespace LawnDart.Demo.Academy.Enrollment;

/// <summary>
/// Enrollment decision for one student and one section.
/// The decision query loads the section and this student's registration events.
/// Seat counts come from every student's seat events. The student's own flags
/// come from that student's events. The append uses the same query.
/// </summary>
public class EnrollmentEntity : DcbEntity<EnrollmentState>
{
    // Parameterless constructor required by IDcbRepository.GetOrCreateEntityAsync<T>() new() constraint.
    public EnrollmentEntity() { }

    /// <summary>
    /// Decision for one student in one section.
    /// The first item is the section tag, so every seat event for that section is included.
    /// The second item is this student's registration and enrollment events.
    /// </summary>
    public static Query EnrollmentDecision(Guid studentId, Guid sectionId) =>
        Query.FromItems(
            QueryItem.ByTags($"section:{sectionId}"),
            QueryItem.ByTypeAndTags(
                [
                    "StudentRegistered",
                    "StudentEnrolled",
                    "RegistrationConfirmationSent",
                    "RegistrationCancelled"
                ],
                [$"student:{studentId}"]));

    // The student/section IDs are read back from the entity state after loading.
    private Guid _studentId => State.StudentId == Guid.Empty ? Guid.Empty : State.StudentId;
    private Guid _sectionId => State.SectionId == Guid.Empty ? Guid.Empty : State.SectionId;

    public void Handle(EnrollStudentCommand cmd)
    {
        if (!State.StudentIsActive)
            throw new InvalidOperationException("Student must be registered before enrolling.");

        if (State.SeatsAvailable <= 0)
            throw new InvalidOperationException(
                $"No seats available in section {_sectionId}. Enrollment denied.");

        if (State.StudentEnrolled)
            throw new InvalidOperationException($"Student {_studentId} is already enrolled in this section.");

        if (State.IsCancelled)
            throw new InvalidOperationException("Enrollment has been cancelled.");

        // Emit SeatReserved and StudentEnrolled atomically in the same round-trip.
        // The AppendCondition set by IDcbRepository ensures these only persist if no
        // concurrent enrollment has consumed the last seat between our read and append.
        Emit(new SeatReserved(Guid.NewGuid(), DateTime.UtcNow, _sectionId, _studentId),
            $"student:{_studentId}", $"section:{_sectionId}", $"course:{cmd.CourseId}");

        Emit(new StudentEnrolled(Guid.NewGuid(), DateTime.UtcNow, _studentId, cmd.CourseId),
            $"student:{_studentId}", $"section:{_sectionId}", $"course:{cmd.CourseId}");
    }

    public void Handle(SendConfirmationCommand cmd)
    {
        if (!State.StudentEnrolled)
            throw new InvalidOperationException("Cannot send confirmation before enrollment is complete.");

        if (State.ConfirmationSent)
            return;

        Emit(new RegistrationConfirmationSent(Guid.NewGuid(), DateTime.UtcNow,
            _studentId, cmd.CourseId, cmd.Email),
            $"student:{_studentId}", $"section:{_sectionId}");
    }

    public void Handle(CancelRegistrationCommand cmd)
    {
        if (!State.StudentEnrolled || State.IsCancelled)
            return;

        Emit(new SeatReleased(Guid.NewGuid(), DateTime.UtcNow, _sectionId, _studentId),
            $"student:{_studentId}", $"section:{_sectionId}");

        Emit(new RegistrationCancelled(Guid.NewGuid(), DateTime.UtcNow,
            _studentId, cmd.CourseId, cmd.Reason),
            $"student:{_studentId}", $"section:{_sectionId}");
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case StudentRegistered e:
                State.StudentId = e.StudentId;
                State.StudentEmail = e.Email;
                State.StudentIsActive = true;
                break;

            case SectionCreated e:
                State.SectionId = e.SectionId;
                State.CourseId = e.CourseId;
                State.SeatsAvailable = e.TotalSeats;
                break;

            case SeatReserved e:
                // Every student's seat event changes the count. Only this student's event changes the flag.
                State.SeatsAvailable = Math.Max(0, State.SeatsAvailable - 1);
                if (e.StudentId == State.StudentId)
                    State.SeatReserved = true;
                break;

            case SeatReleased e:
                State.SeatsAvailable++;
                if (e.StudentId == State.StudentId)
                    State.SeatReserved = false;
                break;

            case StudentEnrolled e when e.StudentId == State.StudentId:
                State.StudentEnrolled = true;
                break;

            case RegistrationConfirmationSent e when e.StudentId == State.StudentId:
                State.ConfirmationSent = true;
                break;

            case RegistrationCancelled e when e.StudentId == State.StudentId:
                State.IsCancelled = true;
                State.StudentEnrolled = false;
                break;
        }
    }
}
