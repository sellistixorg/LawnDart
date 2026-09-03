using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Demo.Academy.Domain.Course.Commands;
using LawnDart.Demo.Academy.Domain.Course.Events;

namespace LawnDart.Demo.Academy.Domain.Course;

/// <summary>
/// Course aggregate — defines the course offering with price and publication status.
/// </summary>
public class Course : AggregateRoot<CourseState>
{
    public Course()
    {
        State = new CourseState();
    }

    public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
    {
        return command switch
        {
            CreateCourseCommand cmd  => HandleCreate(cmd),
            PublishCourseCommand cmd => HandlePublish(cmd),
            UpdateCoursePriceCommand cmd => HandleUpdatePrice(cmd),
            _ => Task.CompletedTask
        };
    }

    private Task HandleCreate(CreateCourseCommand cmd)
    {
        if (State.IsPublished || State.CourseId != Guid.Empty)
            throw new InvalidOperationException("Course already exists.");

        Apply(new CourseCreated(Guid.NewGuid(), DateTime.UtcNow,
            cmd.CourseId, cmd.Title, cmd.Description, cmd.Price));
        return Task.CompletedTask;
    }

    private Task HandlePublish(PublishCourseCommand cmd)
    {
        if (State.IsPublished)
            throw new InvalidOperationException("Course is already published.");

        Apply(new CoursePublished(Guid.NewGuid(), DateTime.UtcNow, cmd.CourseId));
        return Task.CompletedTask;
    }

    private Task HandleUpdatePrice(UpdateCoursePriceCommand cmd)
    {
        if (State.CourseId == Guid.Empty)
            throw new InvalidOperationException("Course does not exist.");

        Apply(new CoursePriceUpdated(Guid.NewGuid(), DateTime.UtcNow, cmd.CourseId, cmd.NewPrice));
        return Task.CompletedTask;
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case CourseCreated e:
                State.CourseId = e.CourseId;
                State.Title = e.Title;
                State.Description = e.Description;
                State.Price = e.Price;
                break;

            case CoursePublished:
                State.IsPublished = true;
                break;

            case CoursePriceUpdated e:
                State.Price = e.NewPrice;
                break;
        }
    }
}
