namespace LawnDart.SqlPerformance.Tests;

public sealed class GeneratorTests
{
    [Fact]
    public void Same_seed_makes_the_same_rows()
    {
        GeneratorChecks.AssertStable();
    }

    [Fact]
    public void Sizes_cover_the_configured_counts()
    {
        Assert.Equal(1_000_000, SizeParser.Parse("1M"));
        Assert.Equal(100_000_000, SizeParser.Parse("100M"));
        Assert.Equal(1_000_000_000, SizeParser.Parse("1B"));
        Assert.Equal(1_000_000, SizeParser.Parse("1000000"));
        Assert.Equal(1_000_000, DatasetPlan.Default.Size);
    }
}
