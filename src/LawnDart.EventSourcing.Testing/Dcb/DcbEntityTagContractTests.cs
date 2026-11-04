using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Extensions.Options;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// Tags written by a dynamic consistency boundary entity. Each new event carries only the
/// tags passed to Emit for that event: what that event is about. The load tags, tags on
/// events read during replay, and tags of other events in the same command are not written.
/// The load tags still form the append condition.
/// </summary>
public abstract class DcbEntityTagContractTests : ContractSuite
{
    protected DcbEntityTagContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task HandleCommandAsync_WritesEachEventWithItsOwnTags()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store);
        var course = Tag("course");
        var student = Tag("student");

        var entity = await repository.GetOrCreateEntityAsync<CourseDecision>([course]);
        await repository.HandleCommandAsync(entity, new OpenCourseAndSubscribe(course, student));

        var events = (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(course)))).Events;
        Assert.Equal(2, events.Count);

        var created = Assert.Single(events, e => e.Event is ContractCourseCreated);
        var subscribed = Assert.Single(events, e => e.Event is ContractStudentSubscribed);
        Assert.Equal(new[] { course }, created.Tags.Order(StringComparer.Ordinal));
        Assert.Equal(new[] { course, student }.Order(StringComparer.Ordinal), subscribed.Tags.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task QueryForStudent_DoesNotReturnCourseCreated()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store);
        var course = Tag("course");
        var student = Tag("student");

        var entity = await repository.GetOrCreateEntityAsync<CourseDecision>([course]);
        await repository.HandleCommandAsync(entity, new OpenCourseAndSubscribe(course, student));

        var forStudent = (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(student)))).Events;
        var only = Assert.Single(forStudent);
        Assert.IsType<ContractStudentSubscribed>(only.Event);
    }

    [Fact]
    public async Task HandleCommandAsync_DoesNotWriteTagsReadDuringReplay()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store);
        var course = Tag("course");
        var earlierA = Tag("student");
        var earlierB = Tag("student");
        var student = Tag("student");

        await repository.AppendWithContextAsync([new TaggedEvent(new ContractCourseCreated(Guid.NewGuid(), course), [course])]);
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractStudentSubscribed(Guid.NewGuid(), course, earlierA), [course, earlierA])]);
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractStudentSubscribed(Guid.NewGuid(), course, earlierB), [course, earlierB])]);

        var entity = await repository.GetOrCreateEntityAsync<CourseDecision>([course]);
        Assert.Equal(2, entity.State.Students);
        var before = entity.LastSequencePosition;

        await repository.HandleCommandAsync(entity, new ChangeCapacityAndSubscribe(course, 30, student));

        var written = (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(course)), fromSequencePosition: before + 1)).Events;
        Assert.Equal(2, written.Count);
        Assert.All(written, e =>
        {
            Assert.DoesNotContain(earlierA, e.Tags);
            Assert.DoesNotContain(earlierB, e.Tags);
        });

        var capacity = Assert.Single(written, e => e.Event is ContractCourseCapacityChanged);
        Assert.Equal(new[] { course }, capacity.Tags.Order(StringComparer.Ordinal));
        var subscribed = Assert.Single(written, e => e.Event is ContractStudentSubscribed);
        Assert.Equal(new[] { course, student }.Order(StringComparer.Ordinal), subscribed.Tags.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task UnrelatedDecisionAboutEarlierStudent_DoesNotGetFalseConflict()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store);
        var course = Tag("course");
        var earlier = Tag("student");

        await repository.AppendWithContextAsync([new TaggedEvent(new ContractCourseCreated(Guid.NewGuid(), course), [course])]);
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractStudentSubscribed(Guid.NewGuid(), course, earlier), [course, earlier])]);

        // A decision about the earlier student reads only that student's events.
        var studentDecision = await repository.GetOrCreateEntityAsync<StudentDecision>([earlier]);

        // Meanwhile the course decision replays the earlier student's subscription
        // and changes the course capacity. That event is about the course only.
        var courseDecision = await repository.GetOrCreateEntityAsync<CourseDecision>([course]);
        await repository.HandleCommandAsync(courseDecision, new ChangeCapacity(course, 40));

        // The student decision did not read anything the course decision wrote.
        await repository.HandleCommandAsync(studentDecision, new NoteStudent(earlier, "moved"));

        var forStudent = (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(earlier)))).Events;
        Assert.Equal(2, forStudent.Count);
        Assert.DoesNotContain(forStudent, e => e.Event is ContractCourseCapacityChanged);
    }

    [Fact]
    public async Task HandleCommandAsync_DoesNotWriteLoadTags()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store);
        var course = Tag("course");
        var student = Tag("student");

        // The decision reads by course and student, but the note is about the student only.
        var entity = await repository.GetOrCreateEntityAsync<StudentDecision>([student, course]);
        await repository.HandleCommandAsync(entity, new NoteStudent(student, "hello"));

        var written = Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(student)))).Events);
        Assert.Equal(new[] { student }, written.Tags);
        Assert.Empty((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(course)))).Events);
    }

    [Fact]
    public async Task RelatedDecision_StillConflicts()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store);
        var course = Tag("course");

        await repository.AppendWithContextAsync([new TaggedEvent(new ContractCourseCreated(Guid.NewGuid(), course), [course])]);

        var first = await repository.GetOrCreateEntityAsync<CourseDecision>([course]);
        var second = await repository.GetOrCreateEntityAsync<CourseDecision>([course]);

        await repository.HandleCommandAsync(first, new ChangeCapacity(course, 10));
        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            repository.HandleCommandAsync(second, new ChangeCapacity(course, 20)));
    }

    private static DcbRepository Repo(IEventStore store)
        => new(
            store,
            new ContractMetadataProvider(),
            new ContractTenantContextProvider("test-tenant"),
            Options.Create(new LawnDartOptions()),
            null,
            null,
            null);

    private static string Tag(string kind) => kind + ":" + Guid.NewGuid().ToString("N");
}

[EventTypeName("backend-contract.course-created")]
public sealed record ContractCourseCreated(Guid Id, string Course) : IEvent
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

[EventTypeName("backend-contract.course-capacity-changed")]
public sealed record ContractCourseCapacityChanged(Guid Id, string Course, int Capacity) : IEvent
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

[EventTypeName("backend-contract.student-subscribed")]
public sealed record ContractStudentSubscribed(Guid Id, string Course, string Student) : IEvent
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

[EventTypeName("backend-contract.student-noted")]
public sealed record ContractStudentNoted(Guid Id, string Student, string Note) : IEvent
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

public sealed record OpenCourseAndSubscribe(string Course, string Student) : ICommand
{
    public Guid Id { get; } = Guid.NewGuid();
}

public sealed record ChangeCapacityAndSubscribe(string Course, int Capacity, string Student) : ICommand
{
    public Guid Id { get; } = Guid.NewGuid();
}

public sealed record ChangeCapacity(string Course, int Capacity) : ICommand
{
    public Guid Id { get; } = Guid.NewGuid();
}

public sealed record NoteStudent(string Student, string Note) : ICommand
{
    public Guid Id { get; } = Guid.NewGuid();
}

public sealed class CourseDecisionState : IState
{
    public bool Exists { get; private set; }

    public int Students { get; private set; }

    public int Capacity { get; private set; }

    public void Apply(ContractCourseCreated @event) => Exists = true;

    public void Apply(ContractStudentSubscribed @event) => Students++;

    public void Apply(ContractCourseCapacityChanged @event) => Capacity = @event.Capacity;
}

public sealed class CourseDecision : DcbEntity<CourseDecisionState>
{
    public void Handle(OpenCourseAndSubscribe command)
    {
        Emit(new ContractCourseCreated(Guid.NewGuid(), command.Course), command.Course);
        Emit(new ContractStudentSubscribed(Guid.NewGuid(), command.Course, command.Student), command.Course, command.Student);
    }

    public void Handle(ChangeCapacityAndSubscribe command)
    {
        Emit(new ContractCourseCapacityChanged(Guid.NewGuid(), command.Course, command.Capacity), command.Course);
        Emit(new ContractStudentSubscribed(Guid.NewGuid(), command.Course, command.Student), command.Course, command.Student);
    }

    public void Handle(ChangeCapacity command)
        => Emit(new ContractCourseCapacityChanged(Guid.NewGuid(), command.Course, command.Capacity), command.Course);

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case ContractCourseCreated e: State.Apply(e); break;
            case ContractStudentSubscribed e: State.Apply(e); break;
            case ContractCourseCapacityChanged e: State.Apply(e); break;
        }
    }
}

public sealed class StudentDecisionState : IState
{
    public int Events { get; private set; }

    public void Count() => Events++;
}

public sealed class StudentDecision : DcbEntity<StudentDecisionState>
{
    public void Handle(NoteStudent command)
        => Emit(new ContractStudentNoted(Guid.NewGuid(), command.Student, command.Note), command.Student);

    protected override void ApplyEventToState(IEvent @event) => State.Count();
}
