using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Sdk;

namespace LawnDart.Backends.Contract.Tests.SwapApp;

public sealed record EnrollStudent(Guid Id, Guid StudentId, string Name) : ICommand;

[EventTypeName("contract.student-enrolled")]
public sealed record StudentEnrolled(Guid Id, DateTime Timestamp, Guid StudentId, string Name) : IEvent;

public sealed class StudentState : IState
{
    public string? Name { get; set; }
    public bool Enrolled { get; set; }
}

public sealed class Student : AggregateRoot<StudentState>
{
    public void Handle(EnrollStudent command)
    {
        if (State.Enrolled)
            throw new InvalidOperationException("Student is already enrolled.");

        Apply(new StudentEnrolled(Guid.NewGuid(), DateTime.UtcNow, command.StudentId, command.Name));
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        if (@event is StudentEnrolled enrolled)
        {
            State.Name = enrolled.Name;
            State.Enrolled = true;
        }
    }
}

public sealed class EnrollStudentHandler(IAggregateRepository repository) : ICommandHandler<EnrollStudent>
{
    public async Task HandleAsync(EnrollStudent command, CancellationToken cancellationToken = default)
    {
        var student = await repository.GetOrCreateAsync<Student>(command.StudentId, cancellationToken);
        await repository.HandleCommandAsync(student, command, cancellationToken: cancellationToken);
    }
}

public sealed class StudentRosterView
{
    public string? Name { get; set; }
    public bool Enrolled { get; set; }
}

[SingleStreamProjection("StudentRoster", streamType: "Student")]
public sealed class StudentRosterProjection : ProjectionBase<StudentRosterView>
{
    public void Handle(StudentEnrolled enrolled)
    {
        State.Name = enrolled.Name;
        State.Enrolled = true;
    }
}
