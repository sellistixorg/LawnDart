using LawnDart.Projections.Lightweight.Hosting;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class ProjectionFlushPolicyTests
{
    [Theory]
    [InlineData(false, true, 5000, 10, 1000, false)]  // skip disabled
    [InlineData(true, false, 5000, 10, 1000, false)] // no sequence check
    [InlineData(true, true, 500, 10, 1000, false)]   // behind but under threshold
    [InlineData(true, true, 2000, 10, 1000, true)]    // far behind → skip
    [InlineData(true, true, 1010, 10, 1000, false)]   // exactly at threshold → do not skip
    [InlineData(true, true, 1011, 10, 1000, true)]    // just over threshold → skip
    public void ShouldSkipOpportunisticFlush_MatchesCatchUpRules(
        bool skipEnabled,
        bool supportsSeq,
        long head,
        long last,
        long threshold,
        bool expected)
    {
        var actual = ProjectionFlushPolicy.ShouldSkipOpportunisticFlush(
            skipEnabled, supportsSeq, head, last, threshold);
        Assert.Equal(expected, actual);
    }
}
