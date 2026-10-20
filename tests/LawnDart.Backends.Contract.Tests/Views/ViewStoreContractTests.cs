namespace LawnDart.Backends.Contract.Tests;

public abstract class ViewStoreContractTests : ContractSuite
{
    protected ViewStoreContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task SaveGetDelete_InOneSchema_DoesNotAffectOther()
    {
        var ordering = await Backend.OpenProjectionStoresAsync(ContractStores.Isolated());
        var catalog = await Backend.OpenProjectionStoresAsync(ContractStores.Isolated());
        await ordering.Views.SaveViewAsync("OrderSummary:v1", "instance-1", "{\"n\":1}", checkpoint: 10);
        Assert.Equal("{\"n\":1}", await ordering.Views.GetViewAsync("OrderSummary:v1", "instance-1"));
        Assert.Null(await catalog.Views.GetViewAsync("OrderSummary:v1", "instance-1"));
        await ordering.Views.DeleteViewAsync("OrderSummary:v1", "instance-1");
        Assert.Null(await ordering.Views.GetViewAsync("OrderSummary:v1", "instance-1"));
    }

    [Fact]
    public async Task SaveViewsAsync_BulkUpsert_RoundTripsAndUpdates()
    {
        var views = (await Backend.OpenProjectionStoresAsync(ContractStores.Isolated())).Views;
        var batch = Enumerable.Range(0, 12)
            .Select(i => ($"inst-{i}", $"{{\"n\":{i}}}", (long)(100 + i)))
            .ToList();
        await views.SaveViewsAsync("BulkProj:v1", batch);
        for (var i = 0; i < 12; i++)
        {
            var got = await views.GetViewWithCheckpointAsync("BulkProj:v1", $"inst-{i}");
            Assert.NotNull(got);
            Assert.Equal($"{{\"n\":{i}}}", got!.Value.ViewData);
            Assert.Equal(100 + i, got.Value.Checkpoint);
        }

        await views.SaveViewsAsync("BulkProj:v1",
        [
            ("inst-0", "{\"n\":999}", 999),
            ("inst-11", "{\"n\":1111}", 1111)
        ]);
        var u0 = await views.GetViewWithCheckpointAsync("BulkProj:v1", "inst-0");
        var u11 = await views.GetViewWithCheckpointAsync("BulkProj:v1", "inst-11");
        Assert.Equal("{\"n\":999}", u0!.Value.ViewData);
        Assert.Equal(999, u0.Value.Checkpoint);
        Assert.Equal("{\"n\":1111}", u11!.Value.ViewData);
        Assert.Equal(1111, u11.Value.Checkpoint);
        Assert.Equal("{\"n\":5}", await views.GetViewAsync("BulkProj:v1", "inst-5"));
    }

    [Fact]
    public async Task SaveViewsAsync_Empty_IsNoOp()
    {
        var views = (await Backend.OpenProjectionStoresAsync(ContractStores.Isolated())).Views;
        await views.SaveViewsAsync("Empty:v1", []);
        Assert.Empty(await views.GetViewsByTypeAsync("Empty:v1"));
    }

    [Fact]
    public async Task SaveViewsAsync_DuplicateInstanceId_LastWins()
    {
        var views = (await Backend.OpenProjectionStoresAsync(ContractStores.Isolated())).Views;
        await views.SaveViewsAsync("Dup:v1",
        [
            ("same", "{\"n\":1}", 1),
            ("same", "{\"n\":2}", 2)
        ]);
        var got = await views.GetViewWithCheckpointAsync("Dup:v1", "same");
        Assert.Equal("{\"n\":2}", got!.Value.ViewData);
        Assert.Equal(2, got.Value.Checkpoint);
    }

    [Fact]
    public async Task DeleteAllViews_RemovesThatProjectionOnly()
    {
        var views = (await Backend.OpenProjectionStoresAsync(ContractStores.Isolated())).Views;
        await views.SaveViewAsync("Keep:v1", "a", "{}", 1);
        await views.SaveViewAsync("Drop:v1", "a", "{}", 1);
        await views.DeleteAllViewsAsync("Drop:v1");
        Assert.Null(await views.GetViewAsync("Drop:v1", "a"));
        Assert.Equal("{}", await views.GetViewAsync("Keep:v1", "a"));
    }
}
