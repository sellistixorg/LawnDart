using System.Text;
using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public sealed class EventLogBuffersTagTests
{
    [Fact]
    public void Null_IsEmpty()
        => Assert.Empty(EventLogBuffers.SnapshotTags(null));

    [Fact]
    public void Repeats_AreDropped_FirstCopyAndOrderKept()
    {
        var tags = EventLogBuffers.SnapshotTags([Tag.Parse("b:2"), Tag.Parse("a:1"), Tag.Parse("b:2"), Tag.Parse("c:3"), Tag.Parse("a:1"), Tag.Parse("a:1")]);
        Assert.Equal(["b:2", "a:1", "c:3"], tags.Select(static t => t.ToString()).ToArray());
    }

    [Fact]
    public void Comparison_IsExact()
    {
        var tags = EventLogBuffers.SnapshotTags([Tag.Parse("A:1"), Tag.Parse("a:1"), Tag.Parse("a:A"), Tag.Parse("a:1")]);
        Assert.Equal(["A:1", "a:1", "a:A"], tags.Select(static t => t.ToString()).ToArray());
    }

    [Fact]
    public void NoRepeats_KeepsEveryTagInOrder()
    {
        string[] source = ["z:1", "y:2", "x:3"];
        Assert.Equal(source, EventLogBuffers.SnapshotTags(source.Select(Tag.Parse).ToArray()).Select(static t => t.ToString()).ToArray());
    }

    [Fact]
    public void Copy_DoesNotAliasTheCallersArray()
    {
        string[] source = ["a:1", "a:1"];
        var tags = EventLogBuffers.SnapshotTags(source.Select(Tag.Parse).ToArray());
        source[0] = "changed";
        Assert.Equal(["a:1"], tags.Select(static t => t.ToString()).ToArray());
    }

    [Fact]
    public void LazySequence_IsReadOnce()
    {
        var reads = 0;
        IEnumerable<string> Source()
        {
            reads++;
            yield return "a:1";
            yield return "b:2";
            yield return "a:1";
        }

        Assert.Equal(["a:1", "b:2"], EventLogBuffers.SnapshotTags(Source().Select(Tag.Parse).ToArray()).Select(static t => t.ToString()).ToArray());
        Assert.Equal(1, reads);
    }

    [Fact]
    public void AppendEventAndRecordedEvent_DropRepeats()
    {
        var append = new AppendEvent("T", Encoding.UTF8.GetBytes("{}"), tags: [Tag.Parse("a:1"), Tag.Parse("A:1"), Tag.Parse("a:1")]);
        Assert.Equal(["a:1", "A:1"], append.Tags.Select(static t => t.ToString()).ToArray());

        var recorded = new RecordedEvent("T", Encoding.UTF8.GetBytes("{}"), StreamId.Parse("test:s"), 1, 1, DateTime.UtcNow, tags: [Tag.Parse("x:1"), Tag.Parse("x:1")]);
        Assert.Equal(["x:1"], recorded.Tags.Select(static t => t.ToString()).ToArray());
    }
}
