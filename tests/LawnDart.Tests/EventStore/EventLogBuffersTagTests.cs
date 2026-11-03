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
        var tags = EventLogBuffers.SnapshotTags(["b:2", "a:1", "b:2", "c:3", "a:1", "a:1"]);
        Assert.Equal(["b:2", "a:1", "c:3"], tags);
    }

    [Fact]
    public void Comparison_IsExact()
    {
        var tags = EventLogBuffers.SnapshotTags(["A:1", "a:1", "a:1 ", " a:1", "a:1"]);
        Assert.Equal(["A:1", "a:1", "a:1 ", " a:1"], tags);
    }

    [Fact]
    public void NoRepeats_KeepsEveryTagInOrder()
    {
        string[] source = ["z:1", "y:2", "x:3"];
        Assert.Equal(source, EventLogBuffers.SnapshotTags(source));
    }

    [Fact]
    public void Copy_DoesNotAliasTheCallersArray()
    {
        string[] source = ["a:1", "a:1"];
        var tags = EventLogBuffers.SnapshotTags(source);
        source[0] = "changed";
        Assert.Equal(["a:1"], tags);
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

        Assert.Equal(["a:1", "b:2"], EventLogBuffers.SnapshotTags(Source()));
        Assert.Equal(1, reads);
    }

    [Fact]
    public void AppendEventAndRecordedEvent_DropRepeats()
    {
        var append = new AppendEvent("T", Encoding.UTF8.GetBytes("{}"), tags: ["a:1", "A:1", "a:1"]);
        Assert.Equal(["a:1", "A:1"], append.Tags);

        var recorded = new RecordedEvent("T", Encoding.UTF8.GetBytes("{}"), "s", 1, 1, DateTime.UtcNow, tags: ["x:1", "x:1"]);
        Assert.Equal(["x:1"], recorded.Tags);
    }
}
