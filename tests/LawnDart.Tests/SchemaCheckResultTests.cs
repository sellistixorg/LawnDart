using LawnDart;

namespace LawnDart.Tests;

public class SchemaCheckResultTests
{
    [Fact]
    public void ThrowIfMismatch_DoesNothingWhenTheSchemaMatches()
    {
        var result = SchemaCheckResult.Match("4");

        result.ThrowIfMismatch();

        Assert.True(result.Matches);
        Assert.Equal("4", result.ExpectedFormat);
        Assert.Equal("4", result.FoundFormat);
        Assert.Empty(result.Missing);
    }

    [Fact]
    public void ThrowIfMismatch_ThrowsWhenTheStampOrObjectsDoNotMatch()
    {
        var result = new SchemaCheckResult(false, "4", "2", ["Events", "append_to_stream body"]);

        var ex = Assert.Throws<SchemaMismatchException>(result.ThrowIfMismatch);

        Assert.Same(result, ex.Result);
        Assert.Contains("4", ex.Message, StringComparison.Ordinal);
        Assert.Contains("2", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Events", ex.Message, StringComparison.Ordinal);
        Assert.Contains("append_to_stream body", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ThrowIfMismatch_DescribesAMissingStamp()
    {
        var result = new SchemaCheckResult(false, "1", null, []);

        var ex = Assert.Throws<SchemaMismatchException>(result.ThrowIfMismatch);

        Assert.Contains("no format stamp", ex.Message, StringComparison.Ordinal);
    }
}
