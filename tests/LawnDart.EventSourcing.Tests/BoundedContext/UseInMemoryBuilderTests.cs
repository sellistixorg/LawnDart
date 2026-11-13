using Microsoft.Extensions.DependencyInjection;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventSourcing.Snapshots;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Outbox;
using LawnDart.Snapshots;
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

        services.AddBoundedContext(contextName).UseInMemory().WithEventTypes(typeof(StubEvent));

        return services.BuildServiceProvider();
    }

    [Fact]
    public void UseInMemory_WithoutWithEventTypes_ThrowsOnStoreResolve()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });
        services.AddBoundedContext("default").UseInMemory();

        using var sp = services.BuildServiceProvider();
        var ex = Assert.Throws<InvalidOperationException>(
            () => sp.GetRequiredKeyedService<IEventStore>("default"));
        Assert.Contains("WithEventTypes", ex.Message);
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

        services.AddBoundedContext("ordering").UseInMemory().WithEventTypes(typeof(StubEvent));
        services.AddBoundedContext("catalog").UseInMemory().WithEventTypes(typeof(StubEvent));

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

        services.AddBoundedContext("ordering").UseInMemory().WithEventTypes(typeof(StubEvent));
        services.AddBoundedContext("catalog").UseInMemory().WithEventTypes(typeof(StubEvent));

        var sp = services.BuildServiceProvider();

        var ordering = sp.GetRequiredKeyedService<IEventStore>("ordering");
        var catalog  = sp.GetRequiredKeyedService<IEventStore>("catalog");

        // Append to ordering only
        await ordering.AppendAsync("acme:Order:1", [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);
        await ordering.AppendAsync("acme:Order:2", [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);

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

        var keyedSnap = sp.GetRequiredKeyedService<ISnapshotStore>("default");
        var unkeyedSnap = sp.GetRequiredService<ISnapshotStore>();
        Assert.Same(keyedSnap, unkeyedSnap);
        Assert.IsType<InMemorySnapshotStore>(unkeyedSnap);

        var keyedOutbox = sp.GetRequiredKeyedService<IOutboxWriter>("default");
        var unkeyedOutbox = sp.GetRequiredService<IOutboxWriter>();
        Assert.Same(keyedOutbox, unkeyedOutbox);
        Assert.IsType<InMemoryOutboxWriter>(unkeyedOutbox);
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

    [Fact]
    public async Task UseInMemory_Append_RejectsTenantDefault()
    {
        var services = new ServiceCollection();
        services.AddLawnDart();
        services.AddBoundedContext("default").UseInMemory().WithEventTypes(typeof(StubEvent));
        await using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<IEventStore>();

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync($"default:Order:{Guid.NewGuid()}", [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]));
        Assert.Contains("reserved", ex.Message);
    }

    [Fact]
    public async Task UseInMemory_ValidateNamesFalse_KeepsTenantDefault()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o =>
        {
            o.RequireTenantId = true;
            o.ValidateNames = false;
        });
        services.AddBoundedContext("default").UseInMemory().WithEventTypes(typeof(StubEvent));
        await using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<IEventStore>();
        var streamId = $"default:Order:{Guid.NewGuid()}";

        await store.AppendAsync(
            streamId,
            [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)],
            metadata: new EventMetadata { TenantId = "default" });

        Assert.Single(await store.ReadStreamAsync(streamId));
    }

    [Fact]
    public async Task UseInMemory_RequireTenantIdFalse_AllowsStreamWithoutTenant()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddBoundedContext("default").UseInMemory().WithEventTypes(typeof(StubEvent));
        await using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<IEventStore>();

        await store.AppendAsync("Order:1", [new StubEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        Assert.Single(await store.ReadStreamAsync("Order:1"));
    }

    [Fact]
    public void UseInMemory_EnableOutbox_WithoutPublishEvents_ThrowsOnStoreResolve()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("academy")
            .WithEventTypes(typeof(StubEvent))
            .UseInMemory(o => o.EnableOutbox = true);
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(
            () => provider.GetRequiredKeyedService<IEventStore>("academy"));

        Assert.Contains("academy", ex.Message, StringComparison.Ordinal);
        Assert.Contains("use-in-memory-builder-tests.stub-event", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UseInMemory_EnableOutbox_WritesOnlyDeclaredEvents_AndTheProcessorPublishesThem()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddBoundedContext("academy")
            .WithEventTypes(typeof(StubEvent), typeof(QuietEvent))
            .PublishEvents(events => events.Publish<StubEvent>())
            .UseInMemory(o =>
            {
                o.EnableOutbox = true;
                o.OutboxClaimLease = TimeSpan.Zero;
            });
        await using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredKeyedService<IEventStore>("academy");
        var writer = provider.GetRequiredKeyedService<IOutboxWriter>("academy");
        var declared = new StubEvent(Guid.NewGuid(), DateTime.UtcNow);
        await store.AppendAsync("Order:1", [declared, new QuietEvent(Guid.NewGuid(), DateTime.UtcNow)]);

        var pending = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(declared.Id, pending.Id);
        Assert.Equal("use-in-memory-builder-tests.stub-event", pending.EventType);

        var publisher = new RecordingPublisher();
        var processor = new OutboxProcessor(
            writer,
            publisher,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<OutboxProcessor>.Instance,
            pollingInterval: TimeSpan.FromSeconds(5),
            maxAttempts: 3);
        var batch = typeof(OutboxProcessor).GetMethod(
            "ProcessBatchAsync",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        await (Task)batch!.Invoke(processor, [CancellationToken.None])!;

        Assert.Equal(declared.Id, Assert.Single(publisher.Published).Id);
        Assert.Empty(await writer.GetUnprocessedAsync(10));
    }

    [Fact]
    public async Task UseInMemory_OutboxOff_DoesNotRunTranslations()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddBoundedContext("academy")
            .WithEventTypes(typeof(StubEvent))
            .PublishEvents(events => events.Translate<StubEvent, BrokenNotice>(_ =>
                throw new InvalidOperationException("broken translation")))
            .UseInMemory();
        await using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredKeyedService<IEventStore>("academy");
        var id = Guid.NewGuid();

        await store.AppendAsync("Order:1", [new StubEvent(id, DateTime.UtcNow)]);

        Assert.Equal(id, Assert.Single(await store.ReadStreamAsync("Order:1")).Event.Id);
    }

    // ── Stubs ──────────────────────────────────────────────────────────────────
[EventTypeName("use-in-memory-builder-tests.stub-event")]

    private sealed record StubEvent(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("use-in-memory-builder-tests.quiet-event")]
    private sealed record QuietEvent(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("use-in-memory-builder-tests.broken-notice")]
    private sealed record BrokenNotice(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class RecordingPublisher : IOutboxPublisher
    {
        public List<OutboxMessage> Published { get; } = [];

        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            Published.Add(message);
            return Task.CompletedTask;
        }
    }
}
