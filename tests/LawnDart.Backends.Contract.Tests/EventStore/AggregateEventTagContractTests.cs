using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Tagging;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// An aggregate save writes each event with that event's own tags. Tags that belong to
/// another event in the same save are not copied onto it.
/// </summary>
public abstract class AggregateEventTagContractTests : ContractSuite
{
    protected AggregateEventTagContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task SaveAsync_WritesEachEventWithItsOwnTags()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = new AggregateRepository(
            store,
            new TestMetadataProvider(),
            new TestTenantContextProvider("test-tenant"),
            Options.Create(new LawnDartOptions()),
            new StudentTagProvider());

        var course = Guid.NewGuid();
        var student = "student:" + Guid.NewGuid().ToString("N");
        var aggregate = new CourseAggregate();
        aggregate.SetStreamId($"test-tenant:CourseAggregate:{course}");
        aggregate.Raise(new ContractCourseCreated(Guid.NewGuid(), course.ToString()));
        aggregate.Raise(new ContractStudentSubscribed(Guid.NewGuid(), course.ToString(), student));

        await repository.SaveAsync(aggregate, new CommandMetadata { UserId = "contract", TenantId = "test-tenant" });

        var events = await store.ReadStreamAsync(aggregate.StreamId);
        Assert.Equal(2, events.Count);
        var created = Assert.Single(events, e => e.Event is ContractCourseCreated);
        var subscribed = Assert.Single(events, e => e.Event is ContractStudentSubscribed);

        Assert.DoesNotContain(student, created.Tags);
        Assert.Contains(student, subscribed.Tags);

        // Both events still carry the aggregate tag.
        var aggregateTag = Assert.Single(created.Tags);
        Assert.StartsWith("CourseAggregate:", aggregateTag, StringComparison.Ordinal);
        Assert.Contains(aggregateTag, subscribed.Tags);

        var forStudent = (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(student)))).Events;
        Assert.IsType<ContractStudentSubscribed>(Assert.Single(forStudent).Event);
    }

    private sealed class StudentTagProvider : ITagProvider
    {
        public IEnumerable<string> GetTags(IEvent @event, object? context = null)
            => @event is ContractStudentSubscribed s ? [s.Student] : [];
    }

    private sealed class CourseAggregateState : IState;

    private sealed class CourseAggregate : AggregateRoot<CourseAggregateState>
    {
        public void Raise(IEvent @event) => Apply(@event);

        protected override void ApplyEventToState(IEvent @event)
        {
        }
    }
}
