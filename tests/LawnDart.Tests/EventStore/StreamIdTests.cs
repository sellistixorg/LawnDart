using System.Text.Json;
using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public sealed class StreamIdTests
{
    [Fact]
    public void For_TypeAndId_BuildsTwoPartText()
    {
        var id = StreamId.For("Order", "42");

        Assert.Equal("Order:42", id.ToString());
        Assert.Null(id.TenantId);
        Assert.Equal("Order", id.Type);
        Assert.Equal("42", id.Id);
        Assert.False(id.IsDcb);
    }

    [Fact]
    public void For_TenantTypeAndId_BuildsThreePartText_AndIdMayHoldColons()
    {
        var id = StreamId.For("acme", "Order", "a:b");

        Assert.Equal("acme:Order:a:b", id.ToString());
        Assert.Equal("acme", id.TenantId);
        Assert.Equal("Order", id.Type);
        Assert.Equal("a:b", id.Id);
    }

    [Fact]
    public void Dcb_BuildsTheReservedForm()
    {
        var plain = StreamId.Dcb();
        var tenant = StreamId.Dcb("acme");

        Assert.Equal("dcb", plain.ToString());
        Assert.True(plain.IsDcb);
        Assert.Null(plain.TenantId);
        Assert.Equal("dcb", plain.Type);
        Assert.Equal(string.Empty, plain.Id);

        Assert.Equal("acme:dcb", tenant.ToString());
        Assert.True(tenant.IsDcb);
        Assert.Equal("acme", tenant.TenantId);
    }

    [Theory]
    [InlineData("Order:42", null, "Order", "42", false)]
    [InlineData("acme:Order:42", "acme", "Order", "42", false)]
    [InlineData("acme:Order:a:b:c", "acme", "Order", "a:b:c", false)]
    [InlineData("dcb", null, "dcb", "", true)]
    [InlineData("acme:dcb", "acme", "dcb", "", true)]
    public void Parse_ReadsEveryPart(string text, string? tenant, string type, string id, bool isDcb)
    {
        var parsed = StreamId.Parse(text);

        Assert.Equal(text, parsed.ToString());
        Assert.Equal(tenant, parsed.TenantId);
        Assert.Equal(type, parsed.Type);
        Assert.Equal(id, parsed.Id);
        Assert.Equal(isDcb, parsed.IsDcb);
    }

    [Theory]
    [InlineData("")]
    [InlineData("Order")]
    [InlineData(":42")]
    [InlineData("Order:")]
    [InlineData(" Order:42")]
    [InlineData("Order:42 ")]
    [InlineData("acme::42")]
    [InlineData(":Order:42")]
    [InlineData("acme:Order: 42")]
    [InlineData(" acme:dcb")]
    [InlineData("dcb:42")]
    [InlineData("acme:dcb:42")]
    [InlineData("acme:dcb:dcb")]
    public void Parse_RejectsTextThatBreaksTheRules(string text)
    {
        Assert.Throws<FormatException>(() => StreamId.Parse(text));
        Assert.False(StreamId.TryParse(text, out var parsed));
        Assert.Equal(default, parsed);
    }

    [Fact]
    public void Parse_Null_Throws_AndTryParse_ReturnsFalse()
    {
        Assert.Throws<ArgumentNullException>(() => StreamId.Parse(null!));
        Assert.False(StreamId.TryParse(null, out _));
    }

    [Fact]
    public void Length_IsCapped()
    {
        var longest = "a:" + new string('x', StreamId.MaxLength - 2);
        Assert.Equal(StreamId.MaxLength, StreamId.Parse(longest).ToString().Length);
        Assert.Throws<FormatException>(() => StreamId.Parse(longest + "x"));
        Assert.Throws<ArgumentException>(() => StreamId.For("a", new string('x', StreamId.MaxLength - 1)));
    }

    [Theory]
    [InlineData("", "42")]
    [InlineData("Order", "")]
    [InlineData("Or:der", "42")]
    [InlineData(" Order", "42")]
    [InlineData("Order", "42 ")]
    [InlineData("Order", "a:b")]
    [InlineData("Order", "dcb")]
    [InlineData("dcb", "42")]
    public void For_TypeAndId_RejectsBadParts(string type, string id)
        => Assert.Throws<ArgumentException>(() => StreamId.For(type, id));

    [Theory]
    [InlineData("", "Order", "42")]
    [InlineData("ac:me", "Order", "42")]
    [InlineData("acme", "Or:der", "42")]
    [InlineData("acme", "Order", "")]
    [InlineData("acme ", "Order", "42")]
    [InlineData("acme", "dcb", "42")]
    public void For_TenantTypeAndId_RejectsBadParts(string tenant, string type, string id)
        => Assert.Throws<ArgumentException>(() => StreamId.For(tenant, type, id));

    [Fact]
    public void ThreePartIdDcb_IsAnOrdinaryStream()
    {
        var parsed = StreamId.Parse("acme:Order:dcb");
        Assert.False(parsed.IsDcb);
        Assert.Equal("Order", parsed.Type);
        Assert.Equal("dcb", parsed.Id);
    }

    [Fact]
    public void Dcb_RejectsABadTenant_AndTreatsEmptyAsNoTenant()
    {
        Assert.Equal(StreamId.Dcb(), StreamId.Dcb(""));
        Assert.Throws<ArgumentException>(() => StreamId.Dcb("ac:me"));
        Assert.Throws<ArgumentException>(() => StreamId.Dcb(" acme"));
    }

    [Fact]
    public void BuiltAndParsedValues_AreEqual()
    {
        Assert.Equal(StreamId.For("acme", "Order", "42"), StreamId.Parse("acme:Order:42"));
        Assert.True(StreamId.For("Order", "42") == StreamId.Parse("Order:42"));
        Assert.Equal(StreamId.For("Order", "42").GetHashCode(), StreamId.Parse("Order:42").GetHashCode());
    }

    [Fact]
    public void Comparison_IsOrdinal_AndCaseSensitive()
    {
        var upper = StreamId.Parse("Order:A");
        var lower = StreamId.Parse("Order:a");

        Assert.NotEqual(upper, lower);
        Assert.True(upper < lower);
        Assert.True(lower > upper);
        Assert.True(upper <= StreamId.Parse("Order:A"));
        Assert.True(upper >= StreamId.Parse("Order:A"));
        Assert.True(upper != lower);
        Assert.Equal(-1, Math.Sign(upper.CompareTo(lower)));
    }

    [Fact]
    public void Default_IsEmptyText()
    {
        StreamId empty = default;

        Assert.Equal(string.Empty, empty.ToString());
        Assert.Null(empty.TenantId);
        Assert.Equal(string.Empty, empty.Type);
        Assert.Equal(string.Empty, empty.Id);
        Assert.NotEqual(StreamId.Parse("Order:42"), empty);
    }

    [Fact]
    public void Json_WritesAndReadsTheCanonicalString()
    {
        var id = StreamId.For("acme", "Order", "42");

        var json = JsonSerializer.Serialize(id);
        Assert.Equal("\"acme:Order:42\"", json);
        Assert.Equal(id, JsonSerializer.Deserialize<StreamId>(json));
    }

    [Fact]
    public void Json_WorksAsADictionaryKey()
    {
        var map = new Dictionary<StreamId, int> { [StreamId.For("Order", "42")] = 1 };

        var json = JsonSerializer.Serialize(map);
        Assert.Equal("{\"Order:42\":1}", json);
        Assert.Equal(1, JsonSerializer.Deserialize<Dictionary<StreamId, int>>(json)![StreamId.Parse("Order:42")]);
    }

    [Theory]
    [InlineData("\"Order\"")]
    [InlineData("42")]
    [InlineData("null")]
    public void Json_RejectsTextThatIsNotAStreamId(string json)
        => Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<StreamId>(json));
}
