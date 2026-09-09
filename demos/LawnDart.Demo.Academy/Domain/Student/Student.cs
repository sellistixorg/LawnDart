using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.Academy.Domain.Student.Commands;
using LawnDart.Demo.Academy.Domain.Student.Events;

namespace LawnDart.Demo.Academy.Domain.Student;

/// <summary>
/// Student aggregate — owns the student registration lifecycle.
/// </summary>
public class Student : AggregateRoot<StudentState>
{
    public Student()
    {
        State = new StudentState();
    }

    public void Handle(RegisterStudentCommand cmd)
    {
        if (State.IsActive)
            throw new InvalidOperationException($"Student {cmd.StudentId} is already registered.");

        Apply(new StudentRegistered(Guid.NewGuid(), DateTime.UtcNow, cmd.StudentId, cmd.Name, cmd.Email));
    }

    public void Handle(ProcessPaymentCommand cmd)
    {
        if (!State.IsActive)
            throw new InvalidOperationException("Student must be registered before processing payment.");

        Apply(new StudentPaymentProcessed(Guid.NewGuid(), DateTime.UtcNow,
            cmd.StudentId, cmd.CourseId, cmd.Amount, $"REF-{Guid.NewGuid():N}"));
    }

    public void Handle(EnrollStudentCommand cmd)
    {
        if (!State.IsActive)
            throw new InvalidOperationException("Student must be registered before enrolling.");

        if (State.EnrolledCourseIds.Contains(cmd.CourseId))
            throw new InvalidOperationException($"Student is already enrolled in course {cmd.CourseId}.");

        Apply(new StudentEnrolled(Guid.NewGuid(), DateTime.UtcNow, cmd.StudentId, cmd.CourseId));
    }

    public void Handle(SendConfirmationCommand cmd)
    {
        Apply(new RegistrationConfirmationSent(Guid.NewGuid(), DateTime.UtcNow,
            cmd.StudentId, cmd.CourseId, cmd.Email));
    }

    public void Handle(CancelRegistrationCommand cmd)
    {
        if (State.EnrolledCourseIds.Contains(cmd.CourseId))
        {
            Apply(new RegistrationCancelled(Guid.NewGuid(), DateTime.UtcNow,
                cmd.StudentId, cmd.CourseId, cmd.Reason));
        }
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case StudentRegistered e:
                State.StudentId = e.StudentId;
                State.Name = e.Name;
                State.Email = e.Email;
                State.IsActive = true;
                break;

            case StudentEnrolled e:
                if (!State.EnrolledCourseIds.Contains(e.CourseId))
                    State.EnrolledCourseIds.Add(e.CourseId);
                break;

            case RegistrationCancelled e:
                State.EnrolledCourseIds.Remove(e.CourseId);
                break;
        }
    }
}
