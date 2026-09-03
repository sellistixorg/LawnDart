using LawnDart.Dcb;

namespace LawnDart.Tests.Dcb;

public class DcbSnapshotIdTests
{
    [Fact]
    public void FromLoadTags_IsOrderInvariant()
    {
        var a = DcbSnapshotId.FromLoadTags(["product:sku-1", "warehouse:east"]);
        var b = DcbSnapshotId.FromLoadTags(["warehouse:east", "product:sku-1"]);

        Assert.Equal(a, b);
        Assert.Equal(DcbSnapshotId.HexLength, a.Length);
    }

    [Fact]
    public void FromLoadTags_PipeInSingleTag_IsNotTwoTags()
    {
        var joined = DcbSnapshotId.FromLoadTags(["a|b"]);
        var two = DcbSnapshotId.FromLoadTags(["a", "b"]);

        Assert.NotEqual(joined, two);
    }

    [Fact]
    public void FromLoadTags_LengthIsAlways64_EvenForLongAndSet()
    {
        var tags = Enumerable.Range(0, 40)
            .Select(i => $"dimension:{i}:value-{new string('x', 80)}")
            .ToArray();

        var id = DcbSnapshotId.FromLoadTags(tags);

        Assert.Equal(DcbSnapshotId.HexLength, id.Length);
        Assert.Matches("^[0-9a-f]{64}$", id);
    }

    [Fact]
    public void FromLoadTags_EmptySet_IsStable64Hex()
    {
        var id = DcbSnapshotId.FromLoadTags([]);
        Assert.Equal(DcbSnapshotId.HexLength, id.Length);
        Assert.Equal(id, DcbSnapshotId.FromLoadTags(Array.Empty<string>()));
    }

    [Fact]
    public void FromLoadTags_ThrowsOnNullList()
        => Assert.Throws<ArgumentNullException>(() => DcbSnapshotId.FromLoadTags(null!));

    [Fact]
    public void FromLoadTags_ThrowsOnNullElement()
        => Assert.Throws<ArgumentNullException>(() => DcbSnapshotId.FromLoadTags(["ok", null!]));
}
