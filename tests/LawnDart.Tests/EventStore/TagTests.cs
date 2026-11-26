using System.Text.Json;
using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public sealed class TagTests
{
    [Fact]
    public void Create_BuildsKeyValueText()
    {
        var tag = Tag.Create("order", "42");

        Assert.Equal("order:42", tag.ToString());
        Assert.Equal("order", tag.Key);
        Assert.Equal("42", tag.Value);
    }

    [Fact]
    public void Parse_SplitsAtTheFirstColon_SoTheValueMayHoldColons()
    {
        var tag = Tag.Parse("customer:acme:42");

        Assert.Equal("customer", tag.Key);
        Assert.Equal("acme:42", tag.Value);
        Assert.Equal(Tag.Create("customer", "acme:42"), tag);
    }

    [Theory]
    [InlineData("")]
    [InlineData("order")]
    [InlineData(":42")]
    [InlineData("order:")]
    [InlineData(" order:42")]
    [InlineData("order :42")]
    [InlineData("order: 42")]
    [InlineData("order:42 ")]
    public void Parse_RejectsTextThatBreaksTheRules(string text)
    {
        Assert.Throws<FormatException>(() => Tag.Parse(text));
        Assert.False(Tag.TryParse(text, out var parsed));
        Assert.Equal(default, parsed);
    }

    [Fact]
    public void Parse_Null_Throws_AndTryParse_ReturnsFalse()
    {
        Assert.Throws<ArgumentNullException>(() => Tag.Parse(null!));
        Assert.False(Tag.TryParse(null, out _));
    }

    [Theory]
    [InlineData("", "42")]
    [InlineData("order", "")]
    [InlineData("or:der", "42")]
    [InlineData(" order", "42")]
    [InlineData("order", "42 ")]
    public void Create_RejectsBadParts(string key, string value)
        => Assert.Throws<ArgumentException>(() => Tag.Create(key, value));

    [Fact]
    public void Length_IsCapped()
    {
        var longest = "k:" + new string('x', Tag.MaxLength - 2);
        Assert.Equal(Tag.MaxLength, Tag.Parse(longest).ToString().Length);
        Assert.Throws<FormatException>(() => Tag.Parse(longest + "x"));
        Assert.Throws<ArgumentException>(() => Tag.Create("k", new string('x', Tag.MaxLength - 1)));
    }

    [Fact]
    public void Comparison_IsOrdinal_AndCaseSensitive()
    {
        var upper = Tag.Parse("Order:1");
        var lower = Tag.Parse("order:1");

        Assert.NotEqual(upper, lower);
        Assert.True(upper < lower);
        Assert.True(lower > upper);
        Assert.True(upper <= Tag.Parse("Order:1"));
        Assert.True(upper >= Tag.Parse("Order:1"));
        Assert.True(upper != lower);
        Assert.True(Tag.Create("order", "1") == lower);
        Assert.Equal(Tag.Create("order", "1").GetHashCode(), lower.GetHashCode());
    }

    [Fact]
    public void Default_IsEmptyText()
    {
        Tag empty = default;

        Assert.Equal(string.Empty, empty.ToString());
        Assert.Equal(string.Empty, empty.Key);
        Assert.Equal(string.Empty, empty.Value);
    }

    [Fact]
    public void Default_IsRejectedWhereATagIsRequired()
    {
        Assert.Throws<ArgumentException>(() => new TaggedEvent(new TestTagEvent(), default(Tag)));
        Assert.Throws<ArgumentException>(() => new AppendEvent("t", new byte[] { 1 }, tags: [default]));
        Assert.Throws<ArgumentException>(() => QueryItem.ByTags(default(Tag)));
    }

    [Fact]
    public void Json_WritesAndReadsTheCanonicalString()
    {
        var tag = Tag.Create("order", "42");

        var json = JsonSerializer.Serialize(new[] { tag });
        Assert.Equal("[\"order:42\"]", json);
        Assert.Equal(tag, JsonSerializer.Deserialize<Tag[]>(json)![0]);
    }

    [Fact]
    public void Json_WorksAsADictionaryKey()
    {
        var map = new Dictionary<Tag, int> { [Tag.Create("order", "42")] = 1 };

        var json = JsonSerializer.Serialize(map);
        Assert.Equal("{\"order:42\":1}", json);
        Assert.Equal(1, JsonSerializer.Deserialize<Dictionary<Tag, int>>(json)![Tag.Parse("order:42")]);
    }

    [Theory]
    [InlineData("\"order\"")]
    [InlineData("42")]
    [InlineData("null")]
    public void Json_RejectsTextThatIsNotATag(string json)
        => Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<Tag>(json));

    private sealed record TestTagEvent : IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();

        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    }
}
