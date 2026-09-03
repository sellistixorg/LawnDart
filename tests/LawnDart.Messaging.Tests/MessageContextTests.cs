using LawnDart.Messaging;

namespace LawnDart.Messaging.Tests;

public class MessageContextTests
{
    [Fact]
    public void New_GeneratesNonNullMessageId()
    {
        var ctx = MessageContext.New();
        Assert.NotNull(ctx.MessageId);
        Assert.NotEmpty(ctx.MessageId);
    }

    [Fact]
    public void New_EachCallGeneratesUniqueId()
    {
        var a = MessageContext.New();
        var b = MessageContext.New();
        Assert.NotEqual(a.MessageId, b.MessageId);
    }

    [Fact]
    public void CreateChild_SetsCorrelationIdToParentMessageId_WhenNoCorrelationSet()
    {
        var parent = new MessageContext { MessageId = "parent-id" };
        var child = parent.CreateChild();

        Assert.Equal("parent-id", child.CorrelationId);
        Assert.Equal("parent-id", child.CausationId);
    }

    [Fact]
    public void CreateChild_PreservesParentCorrelationId_WhenSet()
    {
        var parent = new MessageContext
        {
            MessageId = "parent-id",
            CorrelationId = "saga-corr-id"
        };
        var child = parent.CreateChild();

        Assert.Equal("saga-corr-id", child.CorrelationId);
        Assert.Equal("parent-id", child.CausationId);
    }

    [Fact]
    public void CreateChild_PropagatesTenantIdAndUserId()
    {
        var parent = new MessageContext
        {
            MessageId = "p-id",
            TenantId = "tenant-1",
            UserId = "user-42"
        };
        var child = parent.CreateChild();

        Assert.Equal("tenant-1", child.TenantId);
        Assert.Equal("user-42", child.UserId);
    }

    [Fact]
    public void CreateChild_HasOwnMessageId()
    {
        var parent = new MessageContext { MessageId = "parent-id" };
        var child = parent.CreateChild();

        Assert.NotNull(child.MessageId);
        Assert.NotEqual("parent-id", child.MessageId);
    }
}
