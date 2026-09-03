using Microsoft.AspNetCore.Http;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class ProjectionFreshnessTests
{
    [Fact]
    public void InferReadSource_MapsKnownStores()
    {
        Assert.Equal(ProjectionFreshness.ReadSourceMemory, ProjectionFreshness.InferReadSource(new InMemoryViewStore()));
        Assert.Equal(ProjectionFreshness.ReadSourceSql, ProjectionFreshness.InferReadSource(new SqlViewStore("Server=.;Database=x;TrustServerCertificate=true")));
    }

    [Theory]
    [InlineData("42", true, 42L)]
    [InlineData("0", true, 0L)]
    [InlineData("nope", false, 0L)]
    [InlineData("", false, 0L)]
    public void TryParseMinSequence_ParsesInvariantIntegers(string raw, bool expectedOk, long expectedValue)
    {
        var ctx = new DefaultHttpContext();
        if (raw.Length > 0)
            ctx.Request.QueryString = new QueryString($"?minSequence={raw}");
        else
            ctx.Request.QueryString = new QueryString("?minSequence=");

        var ok = ProjectionFreshness.TryParseMinSequence(ctx.Request, out var value);
        Assert.Equal(expectedOk, ok);
        if (expectedOk)
            Assert.Equal(expectedValue, value);
    }

    [Fact]
    public void TryParseMinSequence_Absent_ReturnsFalse()
    {
        var ctx = new DefaultHttpContext();
        Assert.False(ProjectionFreshness.TryParseMinSequence(ctx.Request, out _));
    }
}
