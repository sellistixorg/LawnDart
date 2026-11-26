using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class TenancyOptionsWiringTests
{
    [EventTypeName("tenancy-options-wiring.test-event")]
    public sealed class TestEvent : IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
        public string Note { get; init; } = "";
    }

    public sealed class TestAggState : IState
    {
        public string? LastNote { get; set; }
    }

    public sealed class TestAgg : AggregateRoot<TestAggState>
    {
        public TestAgg() => State = new TestAggState();
        public void Record(string note) => Apply(new TestEvent { Note = note });
        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is TestEvent e)
                State.LastNote = e.Note;
        }
    }

    public sealed class TestDcbState : IState
    {
        public int Count { get; private set; }
        public void ApplyEventToState(IEvent @event)
        {
            if (@event is TestEvent)
                Count++;
        }
    }

    [Fact]
    public void InMemoryEventStoreOptions_DefaultsAndObsoleteAlias_MapCleanly()
    {
        var opts = new InMemoryEventStoreOptions();
        Assert.Equal(Tenancy.Shared, opts.Tenancy);
        Assert.Null(opts.SingleTenantId);

#pragma warning disable CS0618
        Assert.True(opts.RequireTenantId);
        opts.RequireTenantId = false;
        Assert.Equal(Tenancy.Single, opts.Tenancy);
        Assert.False(opts.RequireTenantId);

        opts.RequireTenantId = true;
        Assert.Equal(Tenancy.Shared, opts.Tenancy);
        Assert.True(opts.RequireTenantId);
#pragma warning restore CS0618
    }

    [Fact]
    public void InMemoryEventStore_RefusesIsolatedAtStartup()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Isolated }));
        Assert.Contains("Isolated", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void InMemoryEventStore_RefusesSharedWithSingleTenantId()
    {
        Assert.Throws<ArgumentException>(() =>
            new InMemoryEventStore(options: new InMemoryEventStoreOptions
            {
                Tenancy = Tenancy.Shared,
                SingleTenantId = "acme"
            }));
    }

    [Fact]
    public async Task UseInMemory_CopiesTenancyAndSingleTenantId_FromLawnDartOptions()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o =>
        {
            o.Tenancy = Tenancy.Single;
            o.SingleTenantId = "Acme_1";
            o.ValidateNames = false;
        });
        services.AddBoundedContext("default").UseInMemory().WithEventTypes(typeof(TestEvent));

        await using var sp = services.BuildServiceProvider();
        var store = Assert.IsType<InMemoryEventStore>(sp.GetRequiredKeyedService<IEventStore>("default"));
        // Verify Single + SingleTenantId = "Acme_1" + ValidateNames = false behavior:
        // "acme!" wouldn't be needed here since "Acme_1" matches; verify 3-part stream with "Acme_1" works
        // and another tenant is rejected.
        var ok = StreamId.Parse("Acme_1:TestAgg:1");
        var wrong = StreamId.Parse("other:TestAgg:1");
        await store.AppendAsync(ok, [new TestEvent()]);
        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(wrong, [new TestEvent()]));
    }

    [Fact]
    public void UseInMemory_RefusesIsolatedAtRegistration()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.Tenancy = Tenancy.Isolated);

        var ex = Assert.Throws<NotSupportedException>(() =>
            services.AddBoundedContext("default").UseInMemory());
        Assert.Contains("Isolated", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AggregateRepository_RespectsSharedAndSingleModes()
    {
        // Shared: uses tenant from ITenantContextProvider
        var sharedStore = new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Shared });
        var sharedRepo = new AggregateRepository(
            sharedStore,
            new DefaultMetadataProvider(),
            new TestTenantContextProvider("acme"),
            Options.Create(new LawnDartOptions { Tenancy = Tenancy.Shared }));

        var sharedAgg = new TestAgg();
        var sharedId = Guid.NewGuid();
        sharedAgg.SetStreamId($"acme:TestAgg:{sharedId}");
        sharedAgg.Record("hello");
        await sharedRepo.SaveAsync(sharedAgg, new CommandMetadata { TenantId = "acme", UserId = "u1" });
        var loadedShared = await sharedRepo.GetAsync<TestAgg>(sharedId);
        Assert.NotNull(loadedShared);
        Assert.Equal("hello", loadedShared!.State.LastNote);

        // Single with SingleTenantId = null: 2-part stream id
        var singleNullStore = new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Single });
        var singleNullRepo = new AggregateRepository(
            singleNullStore,
            new DefaultMetadataProvider(),
            new TestTenantContextProvider(null),
            Options.Create(new LawnDartOptions { Tenancy = Tenancy.Single }));

        var singleAgg = new TestAgg();
        var singleId = Guid.NewGuid();
        singleAgg.SetStreamId($"TestAgg:{singleId}");
        singleAgg.Record("solo");
        await singleNullRepo.SaveAsync(singleAgg, new CommandMetadata { UserId = "u1" });
        var loadedSingle = await singleNullRepo.GetAsync<TestAgg>(singleId);
        Assert.NotNull(loadedSingle);
        Assert.Equal("solo", loadedSingle!.State.LastNote);

        // Single with SingleTenantId = "acme": matches 3-part stream even when tenant provider is null
        var singleFixedStore = new InMemoryEventStore(options: new InMemoryEventStoreOptions
        {
            Tenancy = Tenancy.Single,
            SingleTenantId = "acme"
        });
        var singleFixedRepo = new AggregateRepository(
            singleFixedStore,
            new DefaultMetadataProvider(),
            new TestTenantContextProvider(null),
            Options.Create(new LawnDartOptions { Tenancy = Tenancy.Single, SingleTenantId = "acme" }));

        var fixedAgg = new TestAgg();
        var fixedId = Guid.NewGuid();
        fixedAgg.SetStreamId($"acme:TestAgg:{fixedId}");
        fixedAgg.Record("fixed");
        await singleFixedRepo.SaveAsync(fixedAgg, new CommandMetadata { UserId = "u1" });
        var loadedFixed = await singleFixedRepo.GetAsync<TestAgg>(fixedId);
        Assert.NotNull(loadedFixed);
        Assert.Equal("fixed", loadedFixed!.State.LastNote);
    }

    [Fact]
    public async Task DcbRepository_RespectsSharedAndSingleModes()
    {
        // Shared: GetStateAsync scopes query to active tenant
        var sharedStore = new InMemoryEventStore(options: new InMemoryEventStoreOptions { Tenancy = Tenancy.Shared });
        var sharedRepo = new DcbRepository(
            sharedStore,
            new DefaultMetadataProvider(),
            new TestTenantContextProvider("acme"),
            Options.Create(new LawnDartOptions { Tenancy = Tenancy.Shared }),
            logger: NullLogger<DcbRepository>.Instance);

        var tag = Tag.Parse("order:1");
        await sharedRepo.AppendWithContextAsync([new TaggedEvent(new TestEvent { Note = "a" }, [tag])]);

        var acmeState = await sharedRepo.GetStateAsync<TestDcbState>(Query.FromItems(QueryItem.ByTags(tag)));
        Assert.Equal(1, acmeState.Count);

        var otherRepo = new DcbRepository(
            sharedStore,
            new DefaultMetadataProvider(),
            new TestTenantContextProvider("globex"),
            Options.Create(new LawnDartOptions { Tenancy = Tenancy.Shared }),
            logger: NullLogger<DcbRepository>.Instance);
        var globexState = await otherRepo.GetStateAsync<TestDcbState>(Query.FromItems(QueryItem.ByTags(tag)));
        Assert.Equal(0, globexState.Count);

        // Single with SingleTenantId = "acme": tags events with SingleTenantId when tenant provider is null
        var fixedStore = new InMemoryEventStore(options: new InMemoryEventStoreOptions
        {
            Tenancy = Tenancy.Single,
            SingleTenantId = "acme"
        });
        var fixedRepo = new DcbRepository(
            fixedStore,
            new DefaultMetadataProvider(),
            new TestTenantContextProvider(null),
            Options.Create(new LawnDartOptions { Tenancy = Tenancy.Single, SingleTenantId = "acme" }),
            logger: NullLogger<DcbRepository>.Instance);

        await fixedRepo.AppendWithContextAsync([new TaggedEvent(new TestEvent { Note = "b" }, [tag])]);
        var fixedState = await fixedRepo.GetStateAsync<TestDcbState>(Query.FromItems(QueryItem.ByTags(tag)));
        Assert.Equal(1, fixedState.Count);

        var raw = await fixedStore.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)));
        var recorded = Assert.Single(raw.Events);
        Assert.Equal("acme", recorded.Metadata?.TenantId);
    }
}
