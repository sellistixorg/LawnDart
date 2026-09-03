using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class InMemoryViewStoreSaveViewsTests
{
    [Fact]
    public async Task SaveViewsAsync_WritesAllInstances()
    {
        var store = new InMemoryViewStore();
        var batch = Enumerable.Range(0, 5)
            .Select(i => ($"id-{i}", $"{{\"n\":{i}}}", (long)i))
            .ToList();

        await store.SaveViewsAsync("Proj:v1", batch);

        for (var i = 0; i < 5; i++)
        {
            var json = await store.GetViewAsync("Proj:v1", $"id-{i}");
            Assert.Equal($"{{\"n\":{i}}}", json);
            var withCk = await store.GetViewWithCheckpointAsync("Proj:v1", $"id-{i}");
            Assert.Equal(i, withCk!.Value.Checkpoint);
        }
    }

    [Fact]
    public async Task SaveViewsAsync_Empty_IsNoOp()
    {
        var store = new InMemoryViewStore();
        await store.SaveViewsAsync("Proj:v1", Array.Empty<(string, string, long)>());
        var all = await store.GetViewsByTypeAsync("Proj:v1");
        Assert.Empty(all);
    }

    [Fact]
    public async Task SaveViewsAsync_DuplicateInstanceId_LastWins()
    {
        var store = new InMemoryViewStore();
        await store.SaveViewsAsync("Proj:v1",
        [
            ("same", "{\"n\":1}", 1),
            ("same", "{\"n\":2}", 2)
        ]);

        var json = await store.GetViewAsync("Proj:v1", "same");
        Assert.Equal("{\"n\":2}", json);
    }
}
