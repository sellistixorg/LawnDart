using LawnDart.Projections;

namespace LawnDart.Backends.Contract.Tests;

public abstract class CheckpointStoreContractTests : ContractSuite
{
    protected CheckpointStoreContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task SaveThenGet_ReturnsTheCheckpoint()
    {
        var checkpoints = (await Backend.OpenProjectionStoresAsync(ContractStores.Isolated())).Checkpoints;
        var saved = new ProjectionCheckpoint
        {
            ProjectionType = "OrderSummary:v1",
            NodeId = 0,
            LastSequencePosition = 12,
            LastUpdated = DateTime.UtcNow,
            TotalEventsProcessed = 12
        };
        await checkpoints.SaveCheckpointAsync(saved);
        var loaded = await checkpoints.GetCheckpointAsync("OrderSummary:v1", 0);
        Assert.NotNull(loaded);
        Assert.Equal(12, loaded!.LastSequencePosition);
        Assert.Equal(12, loaded.TotalEventsProcessed);
    }

    [Fact]
    public async Task Save_DoesNotMoveThePositionBackward()
    {
        var checkpoints = (await Backend.OpenProjectionStoresAsync(ContractStores.Isolated())).Checkpoints;
        await checkpoints.SaveCheckpointAsync(new ProjectionCheckpoint
        {
            ProjectionType = "OrderSummary:v1",
            NodeId = 0,
            LastSequencePosition = 20,
            LastUpdated = DateTime.UtcNow,
            TotalEventsProcessed = 20
        });
        await checkpoints.SaveCheckpointAsync(new ProjectionCheckpoint
        {
            ProjectionType = "OrderSummary:v1",
            NodeId = 0,
            LastSequencePosition = 5,
            LastUpdated = DateTime.UtcNow,
            TotalEventsProcessed = 5
        });
        var loaded = await checkpoints.GetCheckpointAsync("OrderSummary:v1", 0);
        Assert.Equal(20, loaded!.LastSequencePosition);
    }

    [Fact]
    public async Task Delete_RemovesOneNodeAndLeavesTheOther()
    {
        var checkpoints = (await Backend.OpenProjectionStoresAsync(ContractStores.Isolated())).Checkpoints;
        await checkpoints.SaveCheckpointAsync(Checkpoint("Cart:v1", 0, 3));
        await checkpoints.SaveCheckpointAsync(Checkpoint("Cart:v1", 1, 4));
        await checkpoints.DeleteCheckpointAsync("Cart:v1", nodeId: 0);
        Assert.Null(await checkpoints.GetCheckpointAsync("Cart:v1", 0));
        Assert.Equal(4, (await checkpoints.GetCheckpointAsync("Cart:v1", 1))!.LastSequencePosition);
    }

    [Fact]
    public async Task Delete_WithoutNode_RemovesEveryNode()
    {
        var checkpoints = (await Backend.OpenProjectionStoresAsync(ContractStores.Isolated())).Checkpoints;
        await checkpoints.SaveCheckpointAsync(Checkpoint("Cart:v1", 0, 3));
        await checkpoints.SaveCheckpointAsync(Checkpoint("Cart:v1", 1, 4));
        await checkpoints.DeleteCheckpointAsync("Cart:v1");
        Assert.Empty(await checkpoints.GetCheckpointsForProjectionAsync("Cart:v1"));
    }

    [Fact]
    public async Task GetAll_ReturnsEveryProjection()
    {
        var checkpoints = (await Backend.OpenProjectionStoresAsync(ContractStores.Isolated())).Checkpoints;
        await checkpoints.SaveCheckpointAsync(Checkpoint("A:v1", 0, 1));
        await checkpoints.SaveCheckpointAsync(Checkpoint("B:v1", 0, 2));
        var all = (await checkpoints.GetAllCheckpointsAsync()).ToList();
        Assert.Contains(all, c => c.ProjectionType == "A:v1" && c.LastSequencePosition == 1);
        Assert.Contains(all, c => c.ProjectionType == "B:v1" && c.LastSequencePosition == 2);
    }

    [Fact]
    public async Task StreamCheckpoint_ReadsTheViewCheckpoint()
    {
        var stores = await Backend.OpenProjectionStoresAsync(ContractStores.Isolated());
        await stores.Views.SaveViewAsync("PerStream:v1", "stream-1", "{}", checkpoint: 44);
        Assert.Equal(44, await stores.Checkpoints.GetStreamCheckpointAsync("PerStream:v1", "stream-1"));
    }

    private static ProjectionCheckpoint Checkpoint(string type, int node, long position)
        => new()
        {
            ProjectionType = type,
            NodeId = node,
            LastSequencePosition = position,
            LastUpdated = DateTime.UtcNow,
            TotalEventsProcessed = position
        };
}
