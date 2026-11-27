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
            QueryItem.ByTags(Tag.Parse("product:sku-1")),
            QueryItem.ByTypeAndTags(["book-borrowed"], [Tag.Parse("member:1")])));
        var b = DcbSnapshotId.FromQuery(Query.FromItems(
            QueryItem.ByTypeAndTags(["book-borrowed"], [Tag.Parse("member:1")]),
            QueryItem.ByTags(Tag.Parse("product:sku-1"))));

        Assert.Equal(a, b);
    }

    [Fact]
    public void FromQuery_TagOrderInsideAnItemDoesNotChangeTheId()
    {
        var a = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(Tag.Parse("product:sku-1"), Tag.Parse("warehouse:east"))));
        var b = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(Tag.Parse("warehouse:east"), Tag.Parse("product:sku-1"))));

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
        var tagsOnly = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(Tag.Parse("member:1"))));
        var typed = DcbSnapshotId.FromQuery(Query.FromItems(
            QueryItem.ByTypeAndTags(["book-borrowed"], [Tag.Parse("member:1")])));

        Assert.NotEqual(tagsOnly, typed);
    }

    [Fact]
    public void FromQuery_PipeInSingleTag_IsNotTwoTags()
    {
        var joined = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(Tag.Parse("tag:a|b"))));
        var two = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(Tag.Parse("tag:a"), Tag.Parse("tag:b"))));

        Assert.NotEqual(joined, two);
        Assert.Equal(DcbSnapshotId.HexLength, joined.Length);
    }

    [Fact]
    public void FromQuery_RepeatedTagAndIdenticalItems_HashAsOne()
    {
        var repeated = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(Tag.Parse("tag:a"), Tag.Parse("tag:a"))));
        var once = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(Tag.Parse("tag:a"))));
        var twoItems = DcbSnapshotId.FromQuery(Query.FromItems(
            QueryItem.ByTags(Tag.Parse("tag:a")),
            QueryItem.ByTags(Tag.Parse("tag:a"))));

        Assert.Equal(once, repeated);
        Assert.Equal(once, twoItems);
    }

    [Fact]
    public void FromQuery_LengthIsAlways64_EvenForLongAndSet()
    {
        var tags = new string[32];
        for (var i = 0; i < tags.Length; i++)
            tags[i] = "k:" + new string('x', 240) + i;

        var id = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(tags.Select(Tag.Parse).ToArray())));

        Assert.Equal(DcbSnapshotId.HexLength, id.Length);
        Assert.Matches("^[0-9a-f]{64}$", id);
    }

    [Fact]
    public void FromQuery_OneItemTagQuery_DoesNotReuseTheOldTagDigest()
    {
        var tags = new[] { "product:sku-1", "warehouse:east" };
        var current = DcbSnapshotId.FromQuery(Query.FromItems(QueryItem.ByTags(tags.Select(Tag.Parse).ToArray())));

        Assert.NotEqual(OldTagDigest(tags), current);
        Assert.Equal(DcbSnapshotId.HexLength, current.Length);
    }

    [Fact]
    public void FromQuery_ThrowsOnNullQuery()
        => Assert.Throws<ArgumentNullException>(() => DcbSnapshotId.FromQuery(null!));

    [Fact]
    public void QueryItem_RejectsDefaultTag()
        => Assert.Throws<ArgumentException>(() =>
            Query.FromItems(new QueryItem { Tags = new Tag[] { Tag.Parse("ok:1"), default } }));


    [Fact]
    public void FromQuery_TenantScopeChangesTheHash()
    {
        var item = QueryItem.ByTypeAndTags(["book-borrowed"], [Tag.Parse("member:1")]);
        var unscoped = DcbSnapshotId.FromQuery(Query.FromItems(item));
        var tenantA = DcbSnapshotId.FromQuery(Query.FromItems(item).ForTenant("acme"));
        var tenantB = DcbSnapshotId.FromQuery(Query.FromItems(item).ForTenant("globex"));
        var allTenants = DcbSnapshotId.FromQuery(Query.ForAllTenants(item));

        Assert.Equal(4, new[] { unscoped, tenantA, tenantB, allTenants }.Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(DcbSnapshotId.HexLength, tenantA.Length);
        Assert.Equal(DcbSnapshotId.HexLength, allTenants.Length);
    }

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
