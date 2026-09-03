using Microsoft.Extensions.DependencyInjection;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class UseInMemoryBuilderTests
{
    private static IServiceProvider BuildSingleContextProvider(string contextName)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });

        services.AddBoundedContext(contextName).UseInMemory();

        return services.BuildServiceProvider();
    }

    [Fact]
    public void UseInMemory_RegistersKeyedIEventStore()
    {
        var sp    = BuildSingleContextProvider("default");
        var store = sp.GetRequiredKeyedService<IEventStore>("default");

        Assert.NotNull(store);
        Assert.IsType<InMemoryEventStore>(store);
    }

    [Fact]
    public void UseInMemory_EventStoreHasCorrectContextName()
    {
        var sp    = BuildSingleContextProvider("ordering");
        var store = (InMemoryEventStore)sp.GetRequiredKeyedService<IEventStore>("ordering");

        Assert.Equal("ordering", store.ContextName);
    }

    [Fact]
    public void UseInMemory_TwoContexts_HaveIndependentStores()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });

        services.AddBoundedContext("ordering").UseInMemory();
        services.AddBoundedContext("catalog").UseInMemory();

        var sp       = services.BuildServiceProvider();
        var ordering = sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalog  = sp.GetRequiredKeyedService<IEventStore>("catalog");

        Assert.NotSame(ordering, catalog);
    }

    [Fact]
    public async Task UseInMemory_TwoContexts_IndependentGlobalSequences()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });

        services.AddBoundedContext("ordering").UseInMemory();
        services.AddBoundedContext("catalog").UseInMemory();

        var sp = services.BuildServiceProvider();

        var ordering = sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalog  = sp.GetRequiredKeyedService<IEventStore>("catalog");

        // Append to ordering only
        await ordering.AppendAsync("Order:1", [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await ordering.AppendAsync("Order:2", [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        var orderingSeq = await ordering.GetCurrentSequenceAsync();
        var catalogSeq  = await catalog.GetCurrentSequenceAsync();

        Assert.Equal(2, orderingSeq);
        Assert.Equal(0, catalogSeq);
    }

    [Fact]
    public void UseInMemory_RegistersIBoundedContextEventStore()
    {
        var sp          = BuildSingleContextProvider("ordering");
        var descriptors = sp.GetServices<IBoundedContextEventStore>().ToList();

        Assert.Single(descriptors);
        Assert.Equal("ordering", descriptors[0].ContextName);
        Assert.NotNull(descriptors[0].EventStore);
    }

    [Fact]
    public void UseInMemory_Default_RegistersUnkeyedAliases_SameInstance()
    {
        var sp = BuildSingleContextProvider("default");

        var keyedStore = sp.GetRequiredKeyedService<IEventStore>("default");
        var unkeyedStore = sp.GetRequiredService<IEventStore>();
        Assert.Same(keyedStore, unkeyedStore);

        var keyedSubs = sp.GetRequiredKeyedService<IEventStoreSubscriptions>("default");
        var unkeyedSubs = sp.GetRequiredService<IEventStoreSubscriptions>();
        Assert.Same(keyedSubs, unkeyedSubs);

        var keyedAgg = sp.GetRequiredKeyedService<IAggregateRepository>("default");
        var unkeyedAgg = sp.GetRequiredService<IAggregateRepository>();
        Assert.NotNull(keyedAgg);
        Assert.NotNull(unkeyedAgg);

        var keyedDcb = sp.GetRequiredKeyedService<IDcbRepository>("default");
        var unkeyedDcb = sp.GetRequiredService<IDcbRepository>();
        Assert.NotNull(keyedDcb);
        Assert.NotNull(unkeyedDcb);
    }

    [Fact]
    public void UseInMemory_NamedContext_DoesNotRegisterUnkeyedIEventStore()
    {
        var sp = BuildSingleContextProvider("ordering");

        Assert.NotNull(sp.GetRequiredKeyedService<IEventStore>("ordering"));
        Assert.Null(sp.GetService<IEventStore>());
        Assert.Null(sp.GetService<IAggregateRepository>());
        Assert.Null(sp.GetService<IDcbRepository>());
    }

    // ── Stubs ──────────────────────────────────────────────────────────────────

    private sealed record StubEvent(Guid Id, DateTime Timestamp) : IEvent;
}
