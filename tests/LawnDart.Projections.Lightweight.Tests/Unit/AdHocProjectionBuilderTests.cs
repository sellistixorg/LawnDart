using System.Text.Json;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.TimeTravelQuery;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

/// <summary>
/// Unit tests for <see cref="AdHocProjectionBuilder"/> covering all four projection kinds,
/// all three cutoff types, and error-handling edge cases.
/// </summary>
public class AdHocProjectionBuilderTests
{
    // ── Test helpers ────────────────────────────────────────────────────────────

    private static EventMetadata MakeMeta(DateTime? timestamp = null) =>
        new() { Timestamp = timestamp ?? DateTime.UtcNow, UserId = "test" };

    private static AdHocProjectionBuilder BuildBuilder(
        InMemoryEventStore store,
        IReadOnlyList<ProjectionRegistration> registrations) =>
        new(store, registrations);

    private static IReadOnlyList<ProjectionRegistration> SingleStreamRegistrations() =>
        [new ProjectionRegistration(
            handlerType: typeof(CounterSummaryProjection),
            viewType: typeof(CounterView),
            projectionName: "CounterSummary",
            kind: ProjectionKind.SingleStream,
            tenantScope: TenantScope.TenantScoped,
            streamType: "Counter")];

    private static IReadOnlyList<ProjectionRegistration> GlobalRegistrations() =>
        [new ProjectionRegistration(
            handlerType: typeof(GlobalTagIndexProjection),
            viewType: typeof(GlobalIndexView),
            projectionName: "GlobalTagIndex",
            kind: ProjectionKind.Global,
            tenantScope: TenantScope.SystemGlobal)];

    private static IReadOnlyList<ProjectionRegistration> DcbRegistrations() =>
        [new ProjectionRegistration(
            handlerType: typeof(CounterDcbFeedProjection),
            viewType: typeof(GlobalIndexView),
            projectionName: "CounterDcbFeed",
            kind: ProjectionKind.Dcb,
            tenantScope: TenantScope.SystemGlobal,
            dcbQueryTypes: [EventTypeNameResolver.GetName(typeof(CounterIncremented))])];

    private static IReadOnlyList<ProjectionRegistration> MultiStreamRegistrations() =>
        [new ProjectionRegistration(
            handlerType: typeof(OrderFulfillmentSummaryProjection),
            viewType: typeof(FulfillmentView),
            projectionName: "OrderFulfillmentSummary",
            kind: ProjectionKind.MultiStream,
            tenantScope: TenantScope.TenantScoped,
            dcbQueryTypes: [
                EventTypeNameResolver.GetName(typeof(OrderCreatedLocal)),
                EventTypeNameResolver.GetName(typeof(ShipmentDispatchedLocal))])];

    private static CounterView Deserialize(TimeTravelResult result) =>
        JsonSerializer.Deserialize<CounterView>(result.ViewJson, ProjectionViewJson.Read)!;

    private static GlobalIndexView DeserializeGlobal(TimeTravelResult result) =>
        JsonSerializer.Deserialize<GlobalIndexView>(result.ViewJson, ProjectionViewJson.Read)!;

    private static FulfillmentView DeserializeFulfillment(TimeTravelResult result) =>
        JsonSerializer.Deserialize<FulfillmentView>(result.ViewJson, ProjectionViewJson.Read)!;

    // ── SingleStream tests ──────────────────────────────────────────────────────

    [Fact]
    public async Task SingleStream_AtVersion_ReturnsPartialState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:abc";
        await store.AppendAsync(streamId, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5),
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 3),
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 7)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var builder = BuildBuilder(store, SingleStreamRegistrations());
        var result = await builder.BuildAtAsync(
            "CounterSummary", streamId, ProjectionCutoff.AtVersion(1), cts.Token);

        var view = Deserialize(result);
        Assert.Equal(5, view.Count);        // only first event
        Assert.Equal(1, result.EventsApplied);
        Assert.Equal(1, result.FinalStreamVersion);
        Assert.Equal("stream version <= 1", result.RequestedCutoff);
    }

    [Fact]
    public async Task SingleStream_AtVersion_FullHistory_ReturnsCompleteState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:def";
        await store.AppendAsync(streamId, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10),
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 20)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var builder = BuildBuilder(store, SingleStreamRegistrations());
        var result = await builder.BuildAtAsync(
            "CounterSummary", streamId, ProjectionCutoff.AtVersion(2), cts.Token);

        var view = Deserialize(result);
        Assert.Equal(30, view.Count);
        Assert.Equal(2, result.EventsApplied);
        Assert.Equal(2, result.FinalStreamVersion);
    }

    [Fact]
    public async Task SingleStream_AtSequence_StopsAtSequenceBoundary()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamA = "tenant1:Counter:stream-a";
        const string streamB = "tenant1:Counter:stream-b";

        // Append 2 events to stream A — these get sequence positions 1 and 2
        await store.AppendAsync(streamA, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10),
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 20)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        // Append 1 event to stream B — gets sequence position 3
        await store.AppendAsync(streamB, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 99)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        // Replay stream A up to sequence 1 — only first event
        var builder = BuildBuilder(store, SingleStreamRegistrations());
        var result = await builder.BuildAtAsync(
            "CounterSummary", streamA, ProjectionCutoff.AtSequence(1), cts.Token);

        var view = Deserialize(result);
        Assert.Equal(10, view.Count);
        Assert.Equal(1, result.EventsApplied);
        Assert.Equal(1, result.FinalSequencePosition);
    }

    [Fact]
    public async Task SingleStream_AtTimestamp_StopsAtTimestampBoundary()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var early = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var late  = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var cutoffTime = new DateTime(2026, 1, 1, 11, 0, 0, DateTimeKind.Utc);

        const string streamId = "tenant1:Counter:timestamp-test";
        await store.AppendAsync(streamId, [
            new CounterIncremented(Guid.NewGuid(), early, 15)
        ], metadata: MakeMeta(early), cancellationToken: cts.Token);

        await store.AppendAsync(streamId, [
            new CounterIncremented(Guid.NewGuid(), late, 30)
        ], metadata: MakeMeta(late), cancellationToken: cts.Token);

        var builder = BuildBuilder(store, SingleStreamRegistrations());
        var result = await builder.BuildAtAsync(
            "CounterSummary", streamId, ProjectionCutoff.AtTimestamp(cutoffTime), cts.Token);

        var view = Deserialize(result);
        Assert.Equal(15, view.Count);       // only the early event
        Assert.Equal(1, result.EventsApplied);
    }

    [Fact]
    public async Task SingleStream_CutoffBeforeAnyEvents_ReturnsDefaultState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        const string streamId = "tenant1:Counter:empty-stream";
        await store.AppendAsync(streamId, [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 42)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var builder = BuildBuilder(store, SingleStreamRegistrations());

        // atVersion 0 means "before event at version 1" — no events applied
        var result = await builder.BuildAtAsync(
            "CounterSummary", streamId, ProjectionCutoff.AtVersion(0), cts.Token);

        var view = Deserialize(result);
        Assert.Equal(0, view.Count);
        Assert.Equal(0, result.EventsApplied);
        Assert.Null(result.FinalStreamVersion);
    }

    // ── Global projection tests ─────────────────────────────────────────────────

    [Fact]
    public async Task Global_AtSequence_ReturnsPartialState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Append 4 events to different streams — each gets a unique sequence position
        await store.AppendAsync("sys:Tags:1", [
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "alpha"),
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "beta")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        await store.AppendAsync("sys:Tags:2", [
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "gamma"),
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "delta")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        // Sequence positions: alpha=1, beta=2, gamma=3, delta=4
        // Replay up to sequence 2 — should see alpha and beta only
        var builder = BuildBuilder(store, GlobalRegistrations());
        var result = await builder.BuildAtAsync(
            "GlobalTagIndex", "global", ProjectionCutoff.AtSequence(2), cts.Token);

        var view = DeserializeGlobal(result);
        Assert.Equal(2, view.TotalTags);
        Assert.Contains("alpha", view.Tags);
        Assert.Contains("beta", view.Tags);
        Assert.DoesNotContain("gamma", view.Tags);
        Assert.Equal(2, result.EventsApplied);
        Assert.Equal(2, result.FinalSequencePosition);
    }

    [Fact]
    public async Task Global_AtTimestamp_ReturnsPartialState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var early  = new DateTime(2026, 3, 1, 9, 0, 0, DateTimeKind.Utc);
        var late   = new DateTime(2026, 3, 1, 15, 0, 0, DateTimeKind.Utc);
        var cutoff = new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Utc);

        await store.AppendAsync("sys:Tags:ts", [
            new GlobalTagged(Guid.NewGuid(), early, "morning")
        ], metadata: MakeMeta(early), cancellationToken: cts.Token);

        await store.AppendAsync("sys:Tags:ts", [
            new GlobalTagged(Guid.NewGuid(), late, "afternoon")
        ], metadata: MakeMeta(late), cancellationToken: cts.Token);

        var builder = BuildBuilder(store, GlobalRegistrations());
        var result = await builder.BuildAtAsync(
            "GlobalTagIndex", "global", ProjectionCutoff.AtTimestamp(cutoff), cts.Token);

        var view = DeserializeGlobal(result);
        Assert.Equal(1, view.TotalTags);
        Assert.Contains("morning", view.Tags);
        Assert.DoesNotContain("afternoon", view.Tags);
    }

    // ── DCB projection tests ────────────────────────────────────────────────────

    [Fact]
    public async Task Dcb_AtSequence_ReturnsFilteredAndBoundedState()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Append a mix: CounterIncremented (matches DCB filter) and GlobalTagged (does not)
        await store.AppendAsync("sys:Counter:dcb-1", [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 5)  // seq=1
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        await store.AppendAsync("sys:Tags:dcb-noise", [
            new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "noise") // seq=2 — filtered out
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        await store.AppendAsync("sys:Counter:dcb-2", [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 10), // seq=3
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 20)  // seq=4
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        // Replay up to sequence 3 — should get seq=1 (5) and seq=3 (10), skip seq=4
        var builder = BuildBuilder(store, DcbRegistrations());
        var result = await builder.BuildAtAsync(
            "CounterDcbFeed", "global", ProjectionCutoff.AtSequence(3), cts.Token);

        var view = DeserializeGlobal(result);
        Assert.Equal(15, view.TotalTags);   // 5 + 10; seq=4 excluded; GlobalTagged not counted
        Assert.Equal(2, result.EventsApplied);
    }

    // ── MultiStream projection tests ────────────────────────────────────────────

    [Fact]
    public async Task MultiStream_AtSequence_ReturnsEntityStateBeforeShipment()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var orderId = Guid.NewGuid();
        const string tenant = "acme";

        // seq=1: order created
        await store.AppendAsync($"{tenant}:OrderAggregate:{orderId}", [
            new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "Alice")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        // seq=2: shipment dispatched
        await store.AppendAsync($"{tenant}:ShipmentAggregate:{orderId}", [
            new ShipmentDispatchedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "FedEx")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var builder = BuildBuilder(store, MultiStreamRegistrations());

        // Replay up to sequence 1 — before shipment
        var instanceId = $"{tenant}:{orderId}"; // TenantScoped MultiStream key
        var result = await builder.BuildAtAsync(
            "OrderFulfillmentSummary", instanceId, ProjectionCutoff.AtSequence(1), cts.Token);

        var view = DeserializeFulfillment(result);
        Assert.Equal(orderId, view.OrderId);
        Assert.Equal("Alice", view.Customer);
        Assert.False(view.IsShipped);
        Assert.Equal(1, result.EventsApplied);
    }

    [Fact]
    public async Task MultiStream_AtSequence_AfterShipment_ShowsShipped()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var orderId = Guid.NewGuid();
        const string tenant = "acme";

        await store.AppendAsync($"{tenant}:OrderAggregate:{orderId}", [
            new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "Bob")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        await store.AppendAsync($"{tenant}:ShipmentAggregate:{orderId}", [
            new ShipmentDispatchedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "UPS")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var builder = BuildBuilder(store, MultiStreamRegistrations());

        var instanceId = $"{tenant}:{orderId}";
        var result = await builder.BuildAtAsync(
            "OrderFulfillmentSummary", instanceId, ProjectionCutoff.AtSequence(2), cts.Token);

        var view = DeserializeFulfillment(result);
        Assert.True(view.IsShipped);
        Assert.Equal("UPS", view.Carrier);
        Assert.Equal(2, result.EventsApplied);
    }

    // ── Error handling tests ────────────────────────────────────────────────────

    [Fact]
    public async Task AtVersion_OnGlobalProjection_ThrowsInvalidOperation()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var builder = BuildBuilder(store, GlobalRegistrations());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            builder.BuildAtAsync(
                "GlobalTagIndex", "global",
                ProjectionCutoff.AtVersion(5),
                cts.Token));
    }

    [Fact]
    public async Task UnknownProjectionName_ThrowsInvalidOperation()
    {
        var store = new InMemoryEventStore();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var builder = BuildBuilder(store, SingleStreamRegistrations());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            builder.BuildAtAsync(
                "DoesNotExist", "some-instance",
                ProjectionCutoff.AtSequence(10),
                cts.Token));
    }
}
