using LawnDart;
using LawnDart.Dcb;
using LawnDart.Demo.Academy.Domain.Student.Commands;
using LawnDart.Demo.Academy.Domain.Student.Events;
using LawnDart.Demo.Academy.Domain.CourseSection.Commands;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;

namespace LawnDart.Demo.Academy.Enrollment;

/// <summary>
/// DCB enrollment entity — spans Student, Course, and CourseSection streams.
/// 
/// The key insight vs. traditional aggregates: this entity reads a consistent snapshot
/// of events from multiple logical "entities" in a single query (tagged with
/// student:{id}, section:{id}), applies AppendCondition to guard seat capacity,
/// and appends all resulting events atomically in one round-trip.
/// 
/// Contrast with ShowcaseA where seat reservation is a separate command
/// that requires its own round-trip and a broker between steps.
/// </summary>
public class EnrollmentEntity : DcbEntity<EnrollmentState>
{
    // Parameterless constructor required by IDcbRepository.GetOrCreateEntityAsync<T>() new() constraint.
    public EnrollmentEntity() { }

    /// <summary>
    /// Tags that define the consistency boundary: events for this student AND this section.
    /// Loading these gives us current enrollment + current seat count atomically.
    /// </summary>
    public static string[] GetTags(Guid studentId, Guid sectionId)
        => [$"student:{studentId}", $"section:{sectionId}"];

    // The student/section IDs are read back from the entity state after loading.
    private Guid _studentId => State.StudentId == Guid.Empty ? Guid.Empty : State.StudentId;
    private Guid _sectionId => State.SectionId == Guid.Empty ? Guid.Empty : State.SectionId;

    public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
    {
        return command switch
        {
            EnrollStudentCommand cmd   => HandleEnrollAsync(cmd),
            SendConfirmationCommand cmd => HandleSendConfirmationAsync(cmd),
            CancelRegistrationCommand cmd => HandleCancelAsync(cmd),
            _ => Task.CompletedTask
        };
    }

    private Task HandleEnrollAsync(EnrollStudentCommand cmd)
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

        return Task.CompletedTask;
    }

    private Task HandleSendConfirmationAsync(SendConfirmationCommand cmd)
    {
        if (!State.StudentEnrolled)
            throw new InvalidOperationException("Cannot send confirmation before enrollment is complete.");

        if (State.ConfirmationSent)
            return Task.CompletedTask;

        Emit(new RegistrationConfirmationSent(Guid.NewGuid(), DateTime.UtcNow,
            _studentId, cmd.CourseId, cmd.Email),
            $"student:{_studentId}", $"section:{_sectionId}");

        return Task.CompletedTask;
    }

    private Task HandleCancelAsync(CancelRegistrationCommand cmd)
    {
        if (!State.StudentEnrolled || State.IsCancelled)
            return Task.CompletedTask;

        Emit(new SeatReleased(Guid.NewGuid(), DateTime.UtcNow, _sectionId, _studentId),
            $"student:{_studentId}", $"section:{_sectionId}");

        Emit(new RegistrationCancelled(Guid.NewGuid(), DateTime.UtcNow,
            _studentId, cmd.CourseId, cmd.Reason),
            $"student:{_studentId}", $"section:{_sectionId}");

        return Task.CompletedTask;
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
                State.SeatsAvailable = Math.Max(0, State.SeatsAvailable - 1);
                if (e.StudentId == State.StudentId)
                    State.SeatReserved = true;
                break;

            case SeatReleased e when e.StudentId == State.StudentId:
                State.SeatsAvailable++;
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
