using Microsoft.Extensions.DependencyInjection;
using LawnDart;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventStore;
using LawnDart.EventSourcing.Snapshots;
using LawnDart.Metadata;
using LawnDart.Outbox;
using LawnDart.Snapshots;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.Snapshots;

public class InMemorySnapshotStoreTests
{
    private static IServiceProvider Build(string contextName)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });
        services.AddBoundedContext(contextName).UseInMemory().WithEventTypes(typeof(CatalogPlaceholderEvent));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task UseInMemory_SaveThenLoad_RoundTripsStreamSnapshot()
    {
        var sp = Build("default");
        var store = sp.GetRequiredService<ISnapshotStore>();

        var state = new SnapState { Name = "book", Count = 3 };
        await store.SaveSnapshotAsync("Book:1", version: 4, globalSequence: 10, state);

        var (loaded, info) = await store.LoadSnapshotAsync<SnapState>("Book:1");
        Assert.NotNull(loaded);
        Assert.Equal("book", loaded!.Name);
        Assert.Equal(3, loaded.Count);
        Assert.NotNull(info);
        Assert.Equal(4, info!.Version);
        Assert.Equal(10, info.GlobalSequence);
    }

    [Fact]
    public async Task UseInMemory_SaveThenLoad_RoundTripsDcbSnapshot()
    {
        var sp = Build("default");
        var store = sp.GetRequiredService<IDcbSnapshotStore>();

        var state = new SnapState { Name = "sku", Count = 7 };
        await store.SaveDcbSnapshotAsync("abc", globalSequence: 22, state);

        var (loaded, info) = await store.LoadDcbSnapshotAsync<SnapState>("abc");
        Assert.NotNull(loaded);
        Assert.Equal("sku", loaded!.Name);
        Assert.Equal(7, loaded.Count);
        Assert.NotNull(info);
        Assert.Equal(22, info!.GlobalSequence);
    }

    [Fact]
    public async Task UseInMemory_Save_ReplacesInPlace()
    {
        var sp = Build("default");
        var store = sp.GetRequiredService<ISnapshotStore>();

        await store.SaveSnapshotAsync("s", 1, 1, new SnapState { Count = 1 });
        await store.SaveSnapshotAsync("s", 2, 2, new SnapState { Count = 2 });

        var (loaded, info) = await store.LoadSnapshotAsync<SnapState>("s");
        Assert.Equal(2, loaded!.Count);
        Assert.Equal(2, info!.Version);
        Assert.Single(await EnumerateAsync(sp.GetRequiredService<ISnapshotAdmin>()));
    }

    [Fact]
    public async Task UseInMemory_Save_DoesNotKeepLiveObject()
    {
        var sp = Build("default");
        var store = sp.GetRequiredService<ISnapshotStore>();

        var state = new SnapState { Count = 1 };
        await store.SaveSnapshotAsync("s", 1, 1, state);
        state.Count = 99;

        var (loaded, _) = await store.LoadSnapshotAsync<SnapState>("s");
        Assert.Equal(1, loaded!.Count);
    }

    [Fact]
    public async Task UseInMemory_TwoContexts_HaveIndependentSnapshotStores()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });
        services.AddBoundedContext("ordering").UseInMemory().WithEventTypes(typeof(CatalogPlaceholderEvent));
        services.AddBoundedContext("catalog").UseInMemory().WithEventTypes(typeof(CatalogPlaceholderEvent));
        var sp = services.BuildServiceProvider();

        var ordering = sp.GetRequiredKeyedService<ISnapshotStore>("ordering");
        var catalog = sp.GetRequiredKeyedService<ISnapshotStore>("catalog");
        Assert.NotSame(ordering, catalog);

        await ordering.SaveSnapshotAsync("s", 1, 1, new SnapState { Count = 4 });
        var (loaded, _) = await catalog.LoadSnapshotAsync<SnapState>("s");
        Assert.Null(loaded);
    }

    [Fact]
    public void UseInMemory_RegistersOutboxWriter()
    {
        var sp = Build("default");
        var writer = sp.GetRequiredService<IOutboxWriter>();
        Assert.IsType<InMemoryOutboxWriter>(writer);
        Assert.Same(writer, sp.GetRequiredKeyedService<IOutboxWriter>("default"));
    }

    [Fact]
    public void UseInMemory_NamedContext_DoesNotRegisterUnkeyedSnapshotOrOutbox()
    {
        var sp = Build("ordering");
        Assert.NotNull(sp.GetRequiredKeyedService<ISnapshotStore>("ordering"));
        Assert.NotNull(sp.GetRequiredKeyedService<IOutboxWriter>("ordering"));
        Assert.Null(sp.GetService<ISnapshotStore>());
        Assert.Null(sp.GetService<IOutboxWriter>());
    }

    [Fact]
    public void UseInMemory_SnapshotStore_IsNotSqlAndHasNoTaskRun()
    {
        var sp = Build("default");
        Assert.IsType<InMemorySnapshotStore>(sp.GetRequiredService<ISnapshotStore>());
        Assert.IsType<InMemorySnapshotStore>(sp.GetRequiredService<IDcbSnapshotStore>());
        Assert.IsType<InMemorySnapshotStore>(sp.GetRequiredService<ISnapshotAdmin>());
    }

    private static async Task<List<string>> EnumerateAsync(ISnapshotAdmin admin)
    {
        var ids = new List<string>();
        await foreach (var id in admin.EnumerateSnapshotStreamsAsync())
            ids.Add(id);
        return ids;
    }

    private sealed class SnapState
    {
        public string Name { get; set; } = "";
        public int Count { get; set; }
    }
}
