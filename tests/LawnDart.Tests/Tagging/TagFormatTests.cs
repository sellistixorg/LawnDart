using LawnDart.Tagging;

namespace LawnDart.Tests.Tagging;

public class TagFormatTests
{
    private static readonly Guid StudentId = Guid.Parse("0ad2f06e-fc65-4394-bfa1-f3116bf68808");

    [Fact]
    public void LowercaseTagFormat_Guid_LowercasesTypeAndUsesFormatD()
    {
        var format = LowercaseTagFormat.Instance;

        Assert.Equal(
            "student:0ad2f06e-fc65-4394-bfa1-f3116bf68808",
            format.Tag("Student", StudentId));
        Assert.Equal(format.Tag("student", StudentId), format.Tag("STUDENT", StudentId.ToString("D")));
    }

    [Fact]
    public void LowercaseTagFormat_StringId_IsKeptAsGiven()
    {
        var tag = LowercaseTagFormat.Instance.Tag("Tenant", "Academy-Tenant");

        Assert.Equal("tenant:Academy-Tenant", tag);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void LowercaseTagFormat_RejectsBlankType(string? type)
    {
        Assert.ThrowsAny<ArgumentException>(() => LowercaseTagFormat.Instance.Tag(type!, StudentId));
        Assert.ThrowsAny<ArgumentException>(() => LowercaseTagFormat.Instance.Tag(type!, "academy-tenant"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void LowercaseTagFormat_RejectsBlankStringId(string? id)
    {
        Assert.ThrowsAny<ArgumentException>(() => LowercaseTagFormat.Instance.Tag("tenant", id!));
    }

    [Fact]
    public void Matches_AcceptsDefaultFormat()
    {
        var format = LowercaseTagFormat.Instance;
        var student = format.Tag("student", StudentId);
        var tenant = format.Tag("tenant", "academy-tenant");

        Assert.True(TagFormatRoundTrip.Matches(format, student));
        Assert.True(TagFormatRoundTrip.Matches(format, tenant));
    }

    [Fact]
    public void Matches_RejectsCaseAndGuidFormatDrift()
    {
        var format = LowercaseTagFormat.Instance;

        Assert.False(TagFormatRoundTrip.Matches(format, $"Student:{StudentId:D}"));
        Assert.False(TagFormatRoundTrip.Matches(format, $"student:{StudentId:N}"));
        Assert.False(TagFormatRoundTrip.Matches(format, "student"));
        Assert.False(TagFormatRoundTrip.Matches(format, "student:"));
        Assert.False(TagFormatRoundTrip.Matches(format, ":student"));
    }

    [Fact]
    public void Matches_TreatsThirtyTwoHexDigitsAsGuid()
    {
        var format = LowercaseTagFormat.Instance;
        var compact = $"student:{StudentId:N}";

        Assert.False(TagFormatRoundTrip.Matches(format, compact));
        Assert.True(TagFormatRoundTrip.Matches(format, format.Tag("student", StudentId)));
    }
}
