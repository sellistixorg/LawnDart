using System.Text;
using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public sealed class EventIdResolverTests
{
    [Fact]
    public void Resolve_WithoutEventId_MintsVersion7()
    {
        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var empty = EventIdResolver.Resolve(ReadOnlyMemory<byte>.Empty);
        var noId = EventIdResolver.Resolve(Json("""{"UserId":"u1"}"""));
        var notAGuid = EventIdResolver.Resolve(Json("""{"EventId":"not-a-guid"}"""));
        var notJson = EventIdResolver.Resolve(Json("not json"));
        var after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        foreach (var id in new[] { empty, noId, notAGuid, notJson })
        {
            AssertRfcVariant(id, 7);
            Assert.InRange(TimeOf(id), before, after);
        }
    }

    [Fact]
    public void Resolve_KeepsCallerId()
    {
        var version4 = Guid.NewGuid();
        var version7 = Guid.CreateVersion7();

        Assert.Equal(version4, EventIdResolver.Resolve(WithId(version4)));
        Assert.Equal(version7, EventIdResolver.Resolve(WithId(version7)));
    }

    [Fact]
    public void Resolve_UsesTheMintWhenGiven()
    {
        var fixedId = Guid.NewGuid();
        Assert.Equal(fixedId, EventIdResolver.Resolve(ReadOnlyMemory<byte>.Empty, () => fixedId));
    }

    [Fact]
    public void ResolveBatch_MintsDistinctVersion7Ids()
    {
        var ids = EventIdResolver.ResolveBatch(Enumerable.Repeat(ReadOnlyMemory<byte>.Empty, 100).ToList());

        Assert.Equal(100, ids.Distinct().Count());
        Assert.All(ids, id => AssertRfcVariant(id, 7));
    }

    [Theory]
    [InlineData(4)]
    [InlineData(7)]
    public void ResolveBatch_CopiedId_FirstKeepsIt_RepeatsKeepVersionVariantAndTime(int version)
    {
        var original = version == 7 ? Guid.CreateVersion7() : Guid.NewGuid();
        var metadata = Enumerable.Repeat(WithId(original), 50).ToList();

        var ids = EventIdResolver.ResolveBatch(metadata);

        Assert.Equal(original, ids[0]);
        Assert.Equal(50, ids.Distinct().Count());
        foreach (var id in ids)
        {
            AssertRfcVariant(id, version);
            Assert.Equal(Bytes(original)[..12], Bytes(id)[..12]);
        }
    }

    [Fact]
    public void ResolveBatch_CopiedId_IsStableAcrossCalls()
    {
        var original = Guid.CreateVersion7();
        var metadata = new List<ReadOnlyMemory<byte>> { WithId(original), ReadOnlyMemory<byte>.Empty, WithId(original), WithId(original) };

        var first = EventIdResolver.ResolveBatch(metadata);
        var second = EventIdResolver.ResolveBatch(metadata);

        Assert.Equal(first[0], second[0]);
        Assert.Equal(first[2], second[2]);
        Assert.Equal(first[3], second[3]);
        Assert.NotEqual(first[1], second[1]);
    }

    [Fact]
    public void ResolveBatch_DerivedIdThatMatchesALaterCallerId_StaysDistinct()
    {
        var original = Guid.CreateVersion7();
        var wouldBeDerived = EventIdResolver.Derive(original, 3);
        var metadata = new List<ReadOnlyMemory<byte>> { WithId(original), WithId(wouldBeDerived), WithId(original) };

        var ids = EventIdResolver.ResolveBatch(metadata);

        Assert.Equal(original, ids[0]);
        Assert.Equal(wouldBeDerived, ids[1]);
        Assert.Equal(EventIdResolver.Derive(original, 4), ids[2]);
        AssertRfcVariant(ids[2], 7);
    }

    [Fact]
    public void Derive_ChangesOnlyTheLastFourBytes()
    {
        var original = Guid.CreateVersion7();
        var seen = new HashSet<Guid> { original };
        for (var salt = 1; salt <= 1000; salt++)
        {
            var derived = EventIdResolver.Derive(original, salt);
            Assert.True(seen.Add(derived));
            Assert.Equal(Bytes(original)[..12], Bytes(derived)[..12]);
            Assert.Equal(derived, EventIdResolver.Derive(original, salt));
        }
    }

    private static ReadOnlyMemory<byte> Json(string text) => Encoding.UTF8.GetBytes(text);

    private static ReadOnlyMemory<byte> WithId(Guid id) => Json($$"""{"EventId":"{{id}}"}""");

    private static byte[] Bytes(Guid id) => id.ToByteArray(bigEndian: true);

    private static void AssertRfcVariant(Guid id, int version)
    {
        Assert.Equal(version, id.Version);
        Assert.Equal(0x80, Bytes(id)[8] & 0xC0);
    }

    private static long TimeOf(Guid id)
    {
        var bytes = Bytes(id);
        long milliseconds = 0;
        for (var i = 0; i < 6; i++)
            milliseconds = (milliseconds << 8) | bytes[i];
        return milliseconds;
    }
}
