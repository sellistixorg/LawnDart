using LawnDart.EventStore;

namespace LawnDart.Messaging.Tests;

public class MessageTypeNamesTests
{
    [Fact]
    public void Require_returns_the_declared_token()
    {
        var name = MessageTypeNames.Require(typeof(NamedMessage));
        Assert.Equal("messaging.named-message", name);
    }

    [Fact]
    public void Require_rejects_a_missing_attribute()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => MessageTypeNames.Require(typeof(BareMessage)));
        Assert.Contains("EventTypeName", ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(BareMessage).FullName!, ex.Message, StringComparison.Ordinal);
        Assert.Contains("CLR class name is not used", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Require_rejects_a_blank_token()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => MessageTypeNames.Require(typeof(BlankMessage)));
        Assert.Contains("EventTypeName", ex.Message, StringComparison.Ordinal);
    }

    [EventTypeName("messaging.named-message")]
    private sealed record NamedMessage(Guid Id, DateTime Timestamp);

    private sealed record BareMessage(Guid Id, DateTime Timestamp);

    [EventTypeName("   ")]
    private sealed record BlankMessage(Guid Id, DateTime Timestamp);
}
