using System.Text.Json;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Integration tests for <see cref="LightweightProjectionRunnerService"/> using the
/// <see cref="ProjectionKind.MultiStream"/> kind, verifying that:
/// <list type="bullet">
///   <item>Events from two distinct stream types are combined into one view per entity.</item>
///   <item>Tenant isolation is enforced when <see cref="TenantScope.TenantScoped"/> is used.</item>
///   <item>Events for an unrelated entity do not bleed into another entity's view.</item>
/// </list>
/// </summary>
[Trait("Category", "Integration")]
public class MultiStreamProjectionRunnerTests
{
    private static readonly LightweightProjectionOptions DefaultOptions = new()
    {
        PollInterval       = TimeSpan.FromMilliseconds(50),
        CheckpointInterval = 10,
        BatchSize          = 100
    };

    private static ProjectionRegistration MakeMultiStreamRegistration() =>
        ProjectionScanner.TryBuildRegistration(typeof(OrderFulfillmentSummaryProjection))!;

    private static (IEventStore store, IViewStore views, ICheckpointStore checkpoints)
        BuildInMemoryStores()
    {
        var views       = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var store       = new LawnDart.EventSourcing.EventStore.InMemoryEventStore();
        return (store, views, checkpoints);
    }

    private static EventMetadata MakeMeta() =>
        new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static async Task<LightweightProjectionRunnerService> StartRunnerAsync(
        ProjectionRegistration reg,
        IEventStore eventStore,
        IViewStore views,
        ICheckpointStore checkpoints,
        CancellationToken ct)
    {
        _ = ct;
        var partitioning = new SingleNodePartitioningService();
        var runner = new LightweightProjectionRunnerService(
            reg, eventStore, views, checkpoints, partitioning, DefaultOptions);
        await runner.StartAsync(CancellationToken.None);
        return runner;
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Regression: filtered global reads can return zero events for a window that contains only
    /// unrelated event types.  The runner must advance by the read window (not jump to head-1)
    /// so matching events later in the log are still processed on cold replay.
    /// </summary>
    [Fact]
    public async Task MultiStream_ColdReplay_WithSparseMatchingEvents_DoesNotSkipTailEvents()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var orderId = Guid.NewGuid();

        const int noiseBatches = 25;
        const int batchSize    = 100;
        var noiseStream = "noise:Counter:noisy";
        for (var i = 0; i < noiseBatches; i++)
        {
            IEvent[] batch = new IEvent[batchSize];
            for (var j = 0; j < batchSize; j++)
                batch[j] = new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 1);

            await store.AppendAsync(noiseStream, batch, metadata: MakeMeta(), cancellationToken: cts.Token);
        }

        await store.AppendAsync(
            $"acme:OrderAggregate:{orderId}",
            [new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "alice")],
            metadata: MakeMeta(),
            cancellationToken: cts.Token);

        await store.AppendAsync(
            $"acme:ShipmentAggregate:{Guid.NewGuid()}",
            [new ShipmentDispatchedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "FedEx")],
            metadata: MakeMeta(),
            cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeMultiStreamRegistration(), store, views, checkpoints, cts.Token);

        var instanceKey = $"acme:{orderId}";
        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "OrderFulfillmentSummary:v1",
            instanceKey,
            TimeSpan.FromSeconds(60),
            json =>
            {
                var v = JsonSerializer.Deserialize<FulfillmentView>(json, ProjectionViewJson.Read);
                return v is not null && v.IsShipped && v.Carrier == "FedEx";
            });

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);

        var view = JsonSerializer.Deserialize<FulfillmentView>(viewJson!, ProjectionViewJson.Read);
        Assert.NotNull(view);
        Assert.Equal(orderId, view!.OrderId);
        Assert.Equal("alice", view.Customer);
        Assert.Equal("FedEx", view.Carrier);
        Assert.True(view.IsShipped);
    }

    [Fact]
    public async Task MultiStream_CombinesEventsFromTwoStreamTypes_IntoOneView()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var orderId = Guid.NewGuid();

        // OrderCreatedLocal comes from OrderAggregate stream
        await store.AppendAsync($"acme:OrderAggregate:{orderId}", [
            new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "alice")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        // ShipmentDispatchedLocal comes from a different ShipmentAggregate stream
        await store.AppendAsync($"acme:ShipmentAggregate:{Guid.NewGuid()}", [
            new ShipmentDispatchedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "FedEx")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeMultiStreamRegistration(), store, views, checkpoints, cts.Token);

        // Instance key for TenantScoped MultiStream = "{tenantId}:{entityId}"
        var instanceKey = $"acme:{orderId}";
        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "OrderFulfillmentSummary:v1",
            instanceKey,
            TimeSpan.FromSeconds(60),
            json =>
            {
                var v = JsonSerializer.Deserialize<FulfillmentView>(json, ProjectionViewJson.Read);
                return v is not null && v.IsShipped && v.Carrier == "FedEx";
            });

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);

        var view = JsonSerializer.Deserialize<FulfillmentView>(viewJson!, ProjectionViewJson.Read);
        Assert.NotNull(view);
        Assert.Equal(orderId, view!.OrderId);
        Assert.Equal("alice", view.Customer);
        Assert.Equal("FedEx", view.Carrier);
        Assert.True(view.IsShipped);
    }

    [Fact]
    public async Task MultiStream_TenantIsolation_AliceAndMariaHaveSeparateViews()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var aliceOrderId = Guid.NewGuid();
        var mariaOrderId = Guid.NewGuid();

        // Alice places an order in tenant "acme"
        await store.AppendAsync($"acme:OrderAggregate:{aliceOrderId}", [
            new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, aliceOrderId, "alice")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        // Maria places an order in tenant "globex"
        await store.AppendAsync($"globex:OrderAggregate:{mariaOrderId}", [
            new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, mariaOrderId, "maria")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        // Only Alice's order gets shipped
        await store.AppendAsync($"acme:ShipmentAggregate:{Guid.NewGuid()}", [
            new ShipmentDispatchedLocal(Guid.NewGuid(), DateTime.UtcNow, aliceOrderId, "UPS")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeMultiStreamRegistration(), store, views, checkpoints, cts.Token);

        var aliceJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "OrderFulfillmentSummary:v1",
            $"acme:{aliceOrderId}",
            TimeSpan.FromSeconds(60),
            json =>
            {
                var v = JsonSerializer.Deserialize<FulfillmentView>(json, ProjectionViewJson.Read);
                return v is not null && v.IsShipped && v.Carrier == "UPS";
            });
        var mariaJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "OrderFulfillmentSummary:v1",
            $"globex:{mariaOrderId}",
            TimeSpan.FromSeconds(60),
            json =>
            {
                var v = JsonSerializer.Deserialize<FulfillmentView>(json, ProjectionViewJson.Read);
                return v is not null && v.Customer == "maria";
            });

        await runner.StopAsync(CancellationToken.None);

        Assert.NotNull(aliceJson);
        Assert.NotNull(mariaJson);

        var aliceView = JsonSerializer.Deserialize<FulfillmentView>(aliceJson!, ProjectionViewJson.Read);
        var mariaView = JsonSerializer.Deserialize<FulfillmentView>(mariaJson!, ProjectionViewJson.Read);

        // Alice's view has the shipment; Maria's does not
        Assert.Equal("alice", aliceView!.Customer);
        Assert.Equal("UPS",   aliceView.Carrier);
        Assert.True(aliceView.IsShipped);

        Assert.Equal("maria", mariaView!.Customer);
        Assert.Null(mariaView.Carrier);
        Assert.False(mariaView.IsShipped);
    }

    [Fact]
    public async Task MultiStream_EventWithNullEntityId_IsSkipped()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // This event type is not handled by the resolver → returns null → should be skipped
        await store.AppendAsync("acme:Counter:xyz", [
            new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, 42)
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner = await StartRunnerAsync(
            MakeMultiStreamRegistration(), store, views, checkpoints, cts.Token);

        // Settle while the runner is live; unmatched types must not create a view.
        await Task.Delay(500, CancellationToken.None);
        await runner.StopAsync(CancellationToken.None);

        // No view should have been created since the event type is not in the query filter
        var allViews = await views.GetViewsByTypeAsync("OrderFulfillmentSummary:v1", CancellationToken.None);
        Assert.Empty(allViews);
    }

    [Fact]
    public async Task MultiStream_WarmRestart_RestoresAndContinues()
    {
        var (store, views, checkpoints) = BuildInMemoryStores();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var orderId = Guid.NewGuid();
        var reg = MakeMultiStreamRegistration();

        // First run: only OrderCreated event
        await store.AppendAsync($"acme:OrderAggregate:{orderId}", [
            new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "alice")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner1 = await StartRunnerAsync(reg, store, views, checkpoints, cts.Token);
        var firstJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "OrderFulfillmentSummary:v1",
            $"acme:{orderId}",
            TimeSpan.FromSeconds(60),
            json => JsonSerializer.Deserialize<FulfillmentView>(json, ProjectionViewJson.Read)?.Customer == "alice");
        Assert.NotNull(firstJson);
        await runner1.StopAsync(CancellationToken.None);

        // Second run: add ShipmentDispatched event; runner should restore saved view
        await store.AppendAsync($"acme:ShipmentAggregate:{Guid.NewGuid()}", [
            new ShipmentDispatchedLocal(Guid.NewGuid(), DateTime.UtcNow, orderId, "DHL")
        ], metadata: MakeMeta(), cancellationToken: cts.Token);

        var runner2 = await StartRunnerAsync(reg, store, views, checkpoints, cts.Token);
        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "OrderFulfillmentSummary:v1",
            $"acme:{orderId}",
            TimeSpan.FromSeconds(60),
            json =>
            {
                var v = JsonSerializer.Deserialize<FulfillmentView>(json, ProjectionViewJson.Read);
                return v is not null && v.Carrier == "DHL";
            });
        await runner2.StopAsync(CancellationToken.None);

        Assert.NotNull(viewJson);
        var view = JsonSerializer.Deserialize<FulfillmentView>(viewJson!, ProjectionViewJson.Read);

        Assert.Equal("alice", view!.Customer);
        Assert.Equal("DHL",   view.Carrier);
    }
}
