using LawnDart.EventSourcing.EventStore;

using LawnDart.EventStore;
namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class InMemoryEventStoreContextNameTests
{
    [Fact]
    public void Constructor_DefaultContextName_IsDefault()
    {
        var store = new InMemoryEventStore();
        Assert.Equal("default", store.ContextName);
    }

    [Fact]
    public void Constructor_ExplicitContextName_IsSet()
    {
        var store = new InMemoryEventStore(contextName: "ordering");
        Assert.Equal("ordering", store.ContextName);
    }

    [Fact]
    public void Constructor_NullContextName_FallsBackToDefault()
    {
        var store = new InMemoryEventStore(contextName: null!);
        Assert.Equal("default", store.ContextName);
    }

    [Fact]
    public void Constructor_WhitespaceContextName_FallsBackToDefault()
    {
        var store = new InMemoryEventStore(contextName: "  ");
        Assert.Equal("default", store.ContextName);
    }

    [Fact]
    public async Task TwoStoresWithDifferentContextNames_HaveIndependentSequences()
    {
        var storeA = new InMemoryEventStore(contextName: "ordering");
        var storeB = new InMemoryEventStore(contextName: "catalog");

        await storeA.AppendAsync("Order:1", [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await storeA.AppendAsync("Order:2", [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await storeB.AppendAsync("Product:1", [new TestEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        Assert.Equal(2, await storeA.GetCurrentSequenceAsync());
        Assert.Equal(1, await storeB.GetCurrentSequenceAsync());
    }
[EventTypeName("in-memory-event-store-context-na.test-event")]

    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}
