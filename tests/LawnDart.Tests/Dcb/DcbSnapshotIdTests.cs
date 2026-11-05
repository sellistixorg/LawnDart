using System.Security.Cryptography;
using System.Text;
using LawnDart.Dcb;
using LawnDart.EventStore;

namespace LawnDart.Tests.Dcb;

public class DcbSnapshotIdTests
{
    [Fact]
    public void FromQuery_ItemOrderDoesNotChangeTheId()
    {
        var a = DcbSnapshotId.FromQuery(Query.FromItems(
            QueryItem.ByTags("product:sku-1"),
            QueryItem.ByTypeAndTags(["book-borrowed"], ["member:1"])));
        var b = DcbSnapshotId.FromQuery(Query.FromItems(
            QueryItem.ByTypeAndTags(["book-borrowed"], ["member:1"]),
            QueryItem.ByTags("product:sku-1")));

        Assert.Equal(a, b);
    }

    [Fact]
    public void FromQuery_TagOrderInsideAnItemDoesNotChangeTheId()
    {
        var a = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags("product:sku-1", "warehouse:east")));
        var b = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags("warehouse:east", "product:sku-1")));

        Assert.Equal(a, b);
    }

    [Fact]
    public void FromQuery_TypeOrderInsideAnItemDoesNotChangeTheId()
    {
        var a = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByType("zeta", "alpha")));
        var b = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByType("alpha", "zeta")));

        Assert.Equal(a, b);
    }

    [Fact]
    public void FromQuery_TypeListChangesTheHash()
    {
        var tagsOnly = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags("member:1")));
        var typed = DcbSnapshotId.FromQuery(Query.FromItems(
            QueryItem.ByTypeAndTags(["book-borrowed"], ["member:1"])));

        Assert.NotEqual(tagsOnly, typed);
    }

    [Fact]
    public void FromQuery_PipeInSingleTag_IsNotTwoTags()
    {
        var joined = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags("a|b")));
        var two = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags("a", "b")));

        Assert.NotEqual(joined, two);
        Assert.Equal(DcbSnapshotId.HexLength, joined.Length);
    }

    [Fact]
    public void FromQuery_RepeatedTagAndIdenticalItems_HashAsOne()
    {
        var repeated = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags("a", "a")));
        var once = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags("a")));
        var twoItems = DcbSnapshotId.FromQuery(Query.FromItems(
            QueryItem.ByTags("a"),
            QueryItem.ByTags("a")));

        Assert.Equal(once, repeated);
        Assert.Equal(once, twoItems);
    }

    [Fact]
    public void FromQuery_LengthIsAlways64_EvenForLongAndSet()
    {
        var tags = new string[32];
        for (var i = 0; i < tags.Length; i++)
            tags[i] = new string('x', 400) + i;

        var id = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(tags)));

        Assert.Equal(DcbSnapshotId.HexLength, id.Length);
        Assert.Matches("^[0-9a-f]{64}$", id);
    }

    [Fact]
    public void FromQuery_OneItemTagQuery_DoesNotReuseTheOldTagDigest()
    {
        var tags = new[] { "product:sku-1", "warehouse:east" };
        var current = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(tags)));

        Assert.NotEqual(OldTagDigest(tags), current);
        Assert.Equal(DcbSnapshotId.HexLength, current.Length);
    }

    [Fact]
    public void FromQuery_ThrowsOnNullQuery()
        => Assert.Throws<ArgumentNullException>(() => DcbSnapshotId.FromQuery(null!));

    [Fact]
    public void FromQuery_ThrowsOnNullTag()
        => Assert.Throws<ArgumentNullException>(() =>
            DcbSnapshotId.FromQuery(Query.FromItems(new QueryItem { Tags = new string?[] { "ok", null! }! })));

    /// <summary>
    /// The previous identity hashed a sorted tag list with no version byte and no items.
    /// A one-item tag query must not land on that digest.
    /// </summary>
    private static string OldTagDigest(IReadOnlyList<string> tags)
    {
        var sorted = tags.Order(StringComparer.Ordinal).ToArray();
        using var ms = new MemoryStream();
        using (var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            bw.Write(sorted.Length);
            foreach (var tag in sorted)
            {
                var bytes = Encoding.UTF8.GetBytes(tag);
                bw.Write(bytes.Length);
                bw.Write(bytes);
            }
        }

        return Convert.ToHexString(SHA256.HashData(ms.ToArray())).ToLowerInvariant();
    }
}
