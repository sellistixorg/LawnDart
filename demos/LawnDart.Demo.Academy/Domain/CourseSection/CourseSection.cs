using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.Academy.Domain.CourseSection.Commands;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;

namespace LawnDart.Demo.Academy.Domain.CourseSection;

/// <summary>
/// CourseSection aggregate — owns seat capacity and reservations.
/// This is the traditional (non-DCB) version; seat contention is handled via
/// version-based optimistic concurrency on the section stream.
/// </summary>
public class CourseSection : AggregateRoot<CourseSectionState>
{
    public CourseSection()
    {
        State = new CourseSectionState();
    }

    public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
    {
        return command switch
        {
            CreateSectionCommand cmd  => HandleCreate(cmd),
            ReserveSeatCommand cmd    => HandleReserveSeat(cmd),
            ReleaseSeatCommand cmd    => HandleReleaseSeat(cmd),
            _ => Task.CompletedTask
        };
    }

    private Task HandleCreate(CreateSectionCommand cmd)
    {
        if (State.SectionId != Guid.Empty)
            throw new InvalidOperationException("Section already exists.");

        if (cmd.TotalSeats <= 0)
            throw new ArgumentException("Section must have at least one seat.", nameof(cmd));

        Apply(new SectionCreated(Guid.NewGuid(), DateTime.UtcNow,
            cmd.SectionId, cmd.CourseId, cmd.Title, cmd.TotalSeats));
        return Task.CompletedTask;
    }

    private Task HandleReserveSeat(ReserveSeatCommand cmd)
    {
        if (State.SeatsAvailable <= 0)
            throw new InvalidOperationException(
                $"No seats available in section {State.SectionId}. ({State.TotalSeats} seats, {State.SeatsReserved} reserved)");

        Apply(new SeatReserved(Guid.NewGuid(), DateTime.UtcNow, cmd.SectionId, cmd.StudentId));
        return Task.CompletedTask;
    }

    private Task HandleReleaseSeat(ReleaseSeatCommand cmd)
    {
        if (State.SeatsReserved <= 0)
            throw new InvalidOperationException("No seats are currently reserved.");

        Apply(new SeatReleased(Guid.NewGuid(), DateTime.UtcNow, cmd.SectionId, cmd.StudentId));
        return Task.CompletedTask;
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case SectionCreated e:
                State.SectionId = e.SectionId;
                State.CourseId = e.CourseId;
                State.Title = e.Title;
                State.TotalSeats = e.TotalSeats;
                State.SeatsReserved = 0;
                break;

            case SeatReserved:
                State.SeatsReserved++;
                break;

            case SeatReleased:
                State.SeatsReserved = Math.Max(0, State.SeatsReserved - 1);
                break;
        }
    }
}
