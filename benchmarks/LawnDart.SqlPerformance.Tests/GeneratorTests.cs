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
        Assert.Equal(10_000_000, SizeParser.Parse("10M"));
        Assert.Equal(10_000_000, SizeParser.Parse("10m"));
        Assert.Equal(10_000_000, DatasetPlan.TenMillion);
        Assert.Equal(100_000_000, SizeParser.Parse("100M"));
        Assert.Equal(1_000_000_000, SizeParser.Parse("1B"));
        Assert.Equal(1_000_000, SizeParser.Parse("1000000"));
        Assert.Equal(1_000_000, DatasetPlan.Default.Size);
    }

    [Fact]
    public void Catalog_exposes_rare_type_and_many_tags()
    {
        var catalog = new EventGenerator(new DatasetPlan(20_000, seed: 7, tenants: 4)).Catalog();
        Assert.Equal(EventGenerator.RareType, catalog.RareType);
        Assert.Contains(EventGenerator.RareType, EventGenerator.EventTypeNames);
        Assert.Equal(TagPicker.ManyTagCount, catalog.ManyTags.Length);
        Assert.Equal("sku:0", catalog.ManyTags[0]);
        Assert.Equal("sku:15", catalog.ManyTags[^1]);
    }
}
