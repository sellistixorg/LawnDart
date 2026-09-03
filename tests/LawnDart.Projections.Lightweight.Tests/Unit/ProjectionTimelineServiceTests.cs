using System.Text.Json;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Debug;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Lightweight.TimeTravelQuery;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

/// <summary>
/// Unit tests for <see cref="ProjectionTimelineService"/> covering timeline pagination,
/// applied-index replay, step-with-diff, and the diff computation helper.
/// </summary>
public class ProjectionTimelineServiceTests
{
    // ── Helpers ────────────────────────────────────────────────────────────────

    private static EventMetadata MakeMeta(DateTime? timestamp = null) =>
        new() { Timestamp = timestamp ?? DateTime.UtcNow, UserId = "test" };

    private static IReadOnlyList<ProjectionRegistration> SingleStreamRegs() =>
    [
        new(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter")
    ];

    private static IReadOnlyList<ProjectionRegistration> GlobalRegs() =>
    [
        new(
            handlerType: typeof(GlobalTagIndexProjection),
            viewType: typeof(GlobalIndexView),
            projectionName: "GlobalTagIndex",
            kind: ProjectionKind.Global,
            tenantScope: TenantScope.SystemGlobal)
    ];

    private static IReadOnlyList<ProjectionRegistration> ListRegs() =>
    [
        new(
            handlerType: typeof(GlobalTagListProjection),
            viewType: typeof(List<string>),
            projectionName: "GlobalTagList",
            kind: ProjectionKind.Global,
            tenantScope: TenantScope.SystemGlobal)
    ];

    private static ProjectionTimelineService BuildService(
        InMemoryEventStore store,
        IReadOnlyList<ProjectionRegistration> regs,
        IViewStore? viewStore = null)
        => new(store, viewStore ?? new InMemoryViewStore(), regs);

    // ── GetTimelinePageAsync ───────────────────────────────────────────────────

    [Fact]
    public async Task GetTimelinePage_SingleStream_ReturnsOnlyAppliedEvents()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:abc";
        const string otherStream = "tenant1:Counter:xyz";

        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)], metadata: MakeMeta(), cancellationToken: cts.Token);
        await store.AppendAsync(otherStream, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 99)], metadata: MakeMeta(), cancellationToken: cts.Token);
        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 2)], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc    = BuildService(store, SingleStreamRegs());
        var result = await svc.GetTimelinePageAsync("CounterSummary", streamId, fromIndex: 0, pageSize: 50, cts.Token);

        Assert.Equal("CounterSummary", result.ProjectionName);
        Assert.Equal(streamId, result.InstanceId);
        Assert.Equal(2, result.Events.Count);
        Assert.Equal(2, result.TotalApplied);

        Assert.Equal(0, result.Events[0].AppliedIndex);
        Assert.Equal(1, result.Events[1].AppliedIndex);
        Assert.Equal(streamId, result.Events[0].StreamId);

        // The event from otherStream must not appear.
        Assert.DoesNotContain(result.Events, e => e.StreamId == otherStream);
    }

    [Fact]
    public async Task GetTimelinePage_GlobalProjection_ReturnsAllEvents()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await store.AppendAsync("s1", [new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "a")], metadata: MakeMeta(), cancellationToken: cts.Token);
        await store.AppendAsync("s2", [new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "b")], metadata: MakeMeta(), cancellationToken: cts.Token);
        await store.AppendAsync("s3", [new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "c")], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc    = BuildService(store, GlobalRegs());
        var result = await svc.GetTimelinePageAsync("GlobalTagIndex", "global", fromIndex: 0, pageSize: 50, cts.Token);

        Assert.Equal(3, result.Events.Count);
        Assert.Equal(3, result.TotalApplied);
        Assert.Equal(nameof(GlobalTagged), result.Events[0].EventType);
    }

    [Fact]
    public async Task GetTimelinePage_PaginationIsCorrect()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:paged";
        for (int i = 0; i < 10; i++)
            await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, i + 1)], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc  = BuildService(store, SingleStreamRegs());

        // Request page starting at index 3 with page size 3 → indices 3, 4, 5.
        var page = await svc.GetTimelinePageAsync("CounterSummary", streamId, fromIndex: 3, pageSize: 3, cts.Token);

        Assert.Equal(3, page.Events.Count);
        Assert.Equal(3, page.Events[0].AppliedIndex);
        Assert.Equal(4, page.Events[1].AppliedIndex);
        Assert.Equal(5, page.Events[2].AppliedIndex);
        Assert.Equal(10, page.TotalApplied);
    }

    [Fact]
    public async Task GetTimelinePage_FromIndexBeyondTotal_ReturnsEmptyPage()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:small";
        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc    = BuildService(store, SingleStreamRegs());
        var result = await svc.GetTimelinePageAsync("CounterSummary", streamId, fromIndex: 99, pageSize: 10, cts.Token);

        Assert.Empty(result.Events);
        Assert.Equal(1, result.TotalApplied);
    }

    // ── BuildAtAppliedIndexAsync ──────────────────────────────────────────────

    [Fact]
    public async Task BuildAtAppliedIndex_Zero_ReturnsEmptyState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:empty";
        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5)], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc    = BuildService(store, SingleStreamRegs());
        var result = await svc.BuildAtAppliedIndexAsync("CounterSummary", streamId, appliedCount: 0, cts.Token);

        Assert.Equal(0, result.EventsApplied);
        var view = JsonSerializer.Deserialize<CounterView>(result.ViewJson, ProjectionViewJson.Read)!;
        Assert.Equal(0, view.Count);
        Assert.Equal(string.Empty, view.StreamId);
    }

    [Fact]
    public async Task BuildAtAppliedIndex_Zero_ListBackedProjection_ReturnsEmptyArray()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await store.AppendAsync("global-tags", [new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "featured")], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc = BuildService(store, ListRegs());
        var result = await svc.BuildAtAppliedIndexAsync("GlobalTagList", "global", appliedCount: 0, cts.Token);

        Assert.Equal(0, result.EventsApplied);
        Assert.Equal("[]", result.ViewJson);
    }

    [Fact]
    public async Task BuildAtAppliedIndex_StopsAtCorrectCount()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:counted";

        for (int i = 0; i < 5; i++)
            await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc    = BuildService(store, SingleStreamRegs());
        var result = await svc.BuildAtAppliedIndexAsync("CounterSummary", streamId, appliedCount: 3, cts.Token);

        Assert.Equal(3, result.EventsApplied);
        var view = JsonSerializer.Deserialize<CounterView>(result.ViewJson, ProjectionViewJson.Read)!;
        Assert.Equal(3, view.Count);
    }

    [Fact]
    public async Task BuildAtAppliedIndex_MoreThanTotal_ReturnsFullState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:full";
        await store.AppendAsync(streamId,
            [
                new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10),
                new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5)
            ],
            metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc    = BuildService(store, SingleStreamRegs());
        var result = await svc.BuildAtAppliedIndexAsync("CounterSummary", streamId, appliedCount: 999, cts.Token);

        var view = JsonSerializer.Deserialize<CounterView>(result.ViewJson, ProjectionViewJson.Read)!;
        Assert.Equal(15, view.Count);
        Assert.Equal(2, result.EventsApplied);
    }

    // ── BuildStepAsync ────────────────────────────────────────────────────────

    [Fact]
    public async Task BuildStep_ForwardFromEmpty_ReturnsFirstEventState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:step";
        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 7)], metadata: MakeMeta(), cancellationToken: cts.Token);
        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 3)], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc = BuildService(store, SingleStreamRegs());

        // Step forward to index 0 (first event).
        var (atTarget, atPrevious, targetEvent) =
            await svc.BuildStepAsync("CounterSummary", streamId, targetAppliedIndex: 0, cts.Token);

        var view = JsonSerializer.Deserialize<CounterView>(atTarget.ViewJson, ProjectionViewJson.Read)!;
        Assert.Equal(7, view.Count);
        Assert.Equal(1, atTarget.EventsApplied);

        // Previous = initial (empty) state — no events applied yet, Count must be 0.
        Assert.NotNull(atPrevious);
        Assert.Equal(0, atPrevious.EventsApplied);
        var prevView = JsonSerializer.Deserialize<CounterView>(atPrevious.ViewJson, ProjectionViewJson.Read)!;
        Assert.Equal(0, prevView.Count);

        Assert.NotNull(targetEvent);
        Assert.Equal(0, targetEvent.AppliedIndex);
        Assert.Equal(nameof(CounterIncremented), targetEvent.EventType);
        Assert.Equal(7, targetEvent.EventJson?.GetProperty("Amount").GetInt32());
    }

    [Fact]
    public async Task BuildStep_ForwardMidStream_CapturesPreviousState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:mid";
        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1)], metadata: MakeMeta(), cancellationToken: cts.Token);
        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 2)], metadata: MakeMeta(), cancellationToken: cts.Token);
        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 4)], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc = BuildService(store, SingleStreamRegs());

        // Step to index 2 (third event). Previous should be state after 2 events (count=3).
        var (atTarget, atPrevious, targetEvent) =
            await svc.BuildStepAsync("CounterSummary", streamId, targetAppliedIndex: 2, cts.Token);

        var viewAfter  = JsonSerializer.Deserialize<CounterView>(atTarget.ViewJson, ProjectionViewJson.Read)!;
        var viewBefore = JsonSerializer.Deserialize<CounterView>(atPrevious!.ViewJson, ProjectionViewJson.Read)!;

        Assert.Equal(7, viewAfter.Count);   // 1 + 2 + 4
        Assert.Equal(3, viewBefore.Count);  // 1 + 2
        Assert.Equal(2, targetEvent!.AppliedIndex);
    }

    [Fact]
    public async Task BuildStep_NegativeIndex_ReturnsEmptyState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:neg";
        await store.AppendAsync(streamId, [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5)], metadata: MakeMeta(), cancellationToken: cts.Token);

        var svc = BuildService(store, SingleStreamRegs());

        var (atTarget, atPrevious, targetEvent) =
            await svc.BuildStepAsync("CounterSummary", streamId, targetAppliedIndex: -1, cts.Token);

        var view = JsonSerializer.Deserialize<CounterView>(atTarget.ViewJson, ProjectionViewJson.Read)!;
        Assert.Equal(0, view.Count);
        Assert.Null(atPrevious);
        Assert.Null(targetEvent);
    }

    // ── ComputeDiff ───────────────────────────────────────────────────────────

    [Fact]
    public void ComputeDiff_DetectsChangedProperty()
    {
        var prev = """{"Count":3,"StreamId":"abc"}""";
        var next = """{"Count":7,"StreamId":"abc"}""";

        var diff = ProjectionTimelineService.ComputeDiff(prev, next);

        Assert.Empty(diff.AddedPaths);
        Assert.Empty(diff.RemovedPaths);
        Assert.Single(diff.ChangedProperties);
        Assert.Equal("Count", diff.ChangedProperties[0].Path);
    }

    [Fact]
    public void ComputeDiff_DetectsAddedProperty()
    {
        var prev = """{"Count":1}""";
        var next = """{"Count":1,"StreamId":"new"}""";

        var diff = ProjectionTimelineService.ComputeDiff(prev, next);

        Assert.Single(diff.AddedPaths);
        Assert.Equal("StreamId", diff.AddedPaths[0]);
        Assert.Empty(diff.RemovedPaths);
        Assert.Empty(diff.ChangedProperties);
    }

    [Fact]
    public void ComputeDiff_DetectsRemovedProperty()
    {
        var prev = """{"Count":1,"StreamId":"old"}""";
        var next = """{"Count":1}""";

        var diff = ProjectionTimelineService.ComputeDiff(prev, next);

        Assert.Empty(diff.AddedPaths);
        Assert.Single(diff.RemovedPaths);
        Assert.Equal("StreamId", diff.RemovedPaths[0]);
    }

    [Fact]
    public void ComputeDiff_NestedObjectChange_UsesDotPath()
    {
        var prev = """{"Address":{"City":"Dallas","Zip":"75001"}}""";
        var next = """{"Address":{"City":"Austin","Zip":"75001"}}""";

        var diff = ProjectionTimelineService.ComputeDiff(prev, next);

        Assert.Single(diff.ChangedProperties);
        Assert.Equal("Address.City", diff.ChangedProperties[0].Path);
    }

    [Fact]
    public void ComputeDiff_IdenticalStates_IsEmpty()
    {
        var json = """{"Count":5,"StreamId":"x"}""";
        var diff = ProjectionTimelineService.ComputeDiff(json, json);

        Assert.True(diff.IsEmpty);
    }

    [Fact]
    public void ComputeDiff_ArrayScalarChange_UsesBracketedPath()
    {
        var prev = """{"Tags":["a","draft","c"]}""";
        var next = """{"Tags":["a","published","c"]}""";

        var diff = ProjectionTimelineService.ComputeDiff(prev, next);

        var change = Assert.Single(diff.ChangedProperties);
        Assert.Equal("Tags[1]", change.Path);
        Assert.Equal("draft", change.Before?.GetString());
        Assert.Equal("published", change.After?.GetString());
    }

    [Fact]
    public void ComputeDiff_ArrayAppend_UsesBracketedAddedPath()
    {
        var prev = """{"Tags":["a"]}""";
        var next = """{"Tags":["a","b"]}""";

        var diff = ProjectionTimelineService.ComputeDiff(prev, next);

        var added = Assert.Single(diff.AddedPaths);
        Assert.Equal("Tags[1]", added);
        Assert.Empty(diff.RemovedPaths);
        Assert.Empty(diff.ChangedProperties);
    }

    [Fact]
    public void ComputeDiff_ArrayRemoval_UsesBracketedRemovedPath()
    {
        var prev = """{"Tags":["a","b"]}""";
        var next = """{"Tags":["a"]}""";

        var diff = ProjectionTimelineService.ComputeDiff(prev, next);

        Assert.Empty(diff.AddedPaths);
        var removed = Assert.Single(diff.RemovedPaths);
        Assert.Equal("Tags[1]", removed);
        Assert.Empty(diff.ChangedProperties);
    }

    [Fact]
    public void ComputeDiff_ArrayOfObjects_RecursesIntoProperties()
    {
        var prev = """{"Orders":[{"OrderId":"1","Status":"Pending","Amount":10.0}]}""";
        var next = """{"Orders":[{"OrderId":"1","Status":"Shipped","Amount":10.0}]}""";

        var diff = ProjectionTimelineService.ComputeDiff(prev, next);

        var change = Assert.Single(diff.ChangedProperties);
        Assert.Equal("Orders[0].Status", change.Path);
        Assert.Equal("Pending", change.Before?.GetString());
        Assert.Equal("Shipped", change.After?.GetString());
    }

    [Fact]
    public void ComputeDiff_TopLevelArray_DoesNotUseBlankPath()
    {
        var prev = """[{"Sku":"sku-1","Price":10.0}]""";
        var next = """[{"Sku":"sku-1","Price":12.0},{"Sku":"sku-2","Price":5.0}]""";

        var diff = ProjectionTimelineService.ComputeDiff(prev, next);

        Assert.Contains(diff.ChangedProperties, change => change.Path == "[0].Price");
        Assert.Contains("[1]", diff.AddedPaths);
        Assert.DoesNotContain(diff.ChangedProperties, change => string.IsNullOrWhiteSpace(change.Path));
        Assert.DoesNotContain(diff.AddedPaths, string.IsNullOrWhiteSpace);
        Assert.DoesNotContain(diff.RemovedPaths, string.IsNullOrWhiteSpace);
    }

    // ── GetInstancePageAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task GetInstancePage_FiltersByTenantPrefix()
    {
        var store     = new InMemoryEventStore();
        var viewStore = new InMemoryViewStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await viewStore.SaveViewAsync("CounterSummary:v1", "acme:Counter:1", "{}", 1, cts.Token);
        await viewStore.SaveViewAsync("CounterSummary:v1", "acme:Counter:2", "{}", 2, cts.Token);
        await viewStore.SaveViewAsync("CounterSummary:v1", "other:Counter:3", "{}", 3, cts.Token);

        var svc    = BuildService(store, SingleStreamRegs(), viewStore);
        var result = await svc.GetInstancePageAsync("CounterSummary", "acme", page: 0, pageSize: 50, cts.Token);

        Assert.Equal(2, result.TotalFiltered);
        Assert.Equal(2, result.Items.Count);
        Assert.All(result.Items, id => Assert.StartsWith("acme:", id));
    }

    [Fact]
    public async Task GetInstancePage_Paginates()
    {
        var store     = new InMemoryEventStore();
        var viewStore = new InMemoryViewStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        for (int i = 1; i <= 5; i++)
            await viewStore.SaveViewAsync("CounterSummary:v1", $"t1:Counter:{i}", "{}", i, cts.Token);

        var svc = BuildService(store, SingleStreamRegs(), viewStore);

        var page0 = await svc.GetInstancePageAsync("CounterSummary", "t1", page: 0, pageSize: 2, cts.Token);
        var page1 = await svc.GetInstancePageAsync("CounterSummary", "t1", page: 1, pageSize: 2, cts.Token);
        var page2 = await svc.GetInstancePageAsync("CounterSummary", "t1", page: 2, pageSize: 2, cts.Token);

        Assert.Equal(2, page0.Items.Count);
        Assert.Equal(2, page1.Items.Count);
        Assert.Single(page2.Items);
        Assert.Equal(5, page0.TotalFiltered);
    }

    [Fact]
    public async Task GetInstancePage_NullTenantId_ReturnsAll()
    {
        var store     = new InMemoryEventStore();
        var viewStore = new InMemoryViewStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await viewStore.SaveViewAsync("GlobalTagIndex:v1", "global", "{}", 1, cts.Token);
        await viewStore.SaveViewAsync("GlobalTagIndex:v1", "global-2", "{}", 2, cts.Token);

        var svc    = BuildService(store, GlobalRegs(), viewStore);
        var result = await svc.GetInstancePageAsync("GlobalTagIndex", null, page: 0, pageSize: 50, cts.Token);

        Assert.Equal(2, result.TotalFiltered);
    }

    // ── Unknown projection ────────────────────────────────────────────────────

    [Fact]
    public async Task GetTimelinePage_UnknownProjection_Throws()
    {
        var store = new InMemoryEventStore();
        var svc   = BuildService(store, SingleStreamRegs());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            svc.GetTimelinePageAsync("DoesNotExist", "id", 0, 10));
    }
}
