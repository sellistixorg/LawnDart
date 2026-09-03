using LawnDart.Projections.Lightweight.Hosting;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class ProjectionReadCacheTests
{
    [Fact]
    public void Set_TryGet_RoundTrips()
    {
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        cache.Set("Counter:v1", "a", "{\"n\":1}", 10);

        Assert.True(cache.TryGet("Counter:v1", "a", out var view));
        Assert.Equal("{\"n\":1}", view.ViewJson);
        Assert.Equal(10, view.Sequence);
    }

    [Fact]
    public void Set_DoesNotOverwriteNewerWithOlder()
    {
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        cache.Set("Counter:v1", "a", "{\"n\":2}", 20);
        cache.Set("Counter:v1", "a", "{\"n\":1}", 10);

        Assert.True(cache.TryGet("Counter:v1", "a", out var view));
        Assert.Equal(20, view.Sequence);
        Assert.Equal("{\"n\":2}", view.ViewJson);
    }

    [Fact]
    public void OfferFromDurable_Hot_PromotesAfterConfiguredHits()
    {
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot, promoteAfterHits: 2);
        cache.OfferFromDurable("Counter:v1", "a", "{\"n\":1}", 5);
        Assert.False(cache.TryGet("Counter:v1", "a", out _));

        cache.OfferFromDurable("Counter:v1", "a", "{\"n\":1}", 5);
        Assert.True(cache.TryGet("Counter:v1", "a", out var view));
        Assert.Equal(5, view.Sequence);
    }

    [Fact]
    public void OfferFromDurable_Resident_PromotesOnFirstHit()
    {
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Resident);
        cache.OfferFromDurable("Counter:v1", "a", "{\"n\":1}", 3);
        Assert.True(cache.TryGet("Counter:v1", "a", out _));
    }

    [Fact]
    public void Off_Mode_IgnoresSetAndGet()
    {
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Off);
        cache.Set("Counter:v1", "a", "{}", 1);
        Assert.False(cache.TryGet("Counter:v1", "a", out _));
    }

    [Fact]
    public void Clear_RemovesOnlyMatchingStorageKey()
    {
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot);
        cache.Set("A:v1", "i1", "{}", 1);
        cache.Set("B:v1", "i1", "{}", 1);

        cache.Clear("A:v1");

        Assert.False(cache.TryGet("A:v1", "i1", out _));
        Assert.True(cache.TryGet("B:v1", "i1", out _));
    }

    [Fact]
    public void MaxEntries_EvictsColdest()
    {
        var cache = new ProjectionReadCache(ProjectionReadCacheMode.Hot, maxEntries: 2);
        cache.Set("P:v1", "a", "a", 1);
        Thread.Sleep(5);
        cache.Set("P:v1", "b", "b", 2);
        Thread.Sleep(5);
        cache.Set("P:v1", "c", "c", 3);

        Assert.Equal(2, cache.Count);
        Assert.False(cache.TryGet("P:v1", "a", out _));
        Assert.True(cache.TryGet("P:v1", "c", out _));
    }
}
