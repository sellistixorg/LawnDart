namespace LawnDart.Messaging.Tests;

public class MessageEnvelopeTests
{
    [Fact]
    public void Write_and_read_keep_user_tenant_and_custom_headers()
    {
        var enqueuedAt = new DateTimeOffset(2026, 4, 1, 12, 30, 0, TimeSpan.Zero);
        var context = new MessageContext
        {
            MessageId = "msg-1",
            CorrelationId = "corr-1",
            CausationId = "cause-1",
            UserId = "user-1",
            TenantId = "tenant-1",
            EnqueuedAt = enqueuedAt,
            Headers = new Dictionary<string, string>
            {
                ["region"] = "east",
                [MessageHeaders.TraceParent] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                [MessageHeaders.OutboxStreamId] = "Order:1",
            },
        };

        var wire = new Dictionary<string, string>(StringComparer.Ordinal);
        MessageEnvelopeHeaders.Write(context, "order-placed", wire);

        Assert.Equal("order-placed", wire[MessageHeaders.MessageType]);
        Assert.Equal("user-1", wire[MessageHeaders.UserId]);
        Assert.Equal("tenant-1", wire[MessageHeaders.TenantId]);
        Assert.Equal("east", wire[MessageHeaders.CustomPrefix + "region"]);
        Assert.False(wire.ContainsKey("region"));
        Assert.Equal(context.Headers[MessageHeaders.TraceParent], wire[MessageHeaders.TraceParent]);
        Assert.Equal("Order:1", wire[MessageHeaders.OutboxStreamId]);
        Assert.Equal("outbox.streamId", MessageHeaders.OutboxStreamId);
        Assert.Equal("outbox.sequencePosition", MessageHeaders.OutboxSequencePosition);
        Assert.Equal("outbox.eventType", MessageHeaders.OutboxEventType);

        var restored = MessageEnvelopeHeaders.Read(wire, "InMemory");
        Assert.Equal("order-placed", restored.MessageType);
        Assert.Equal("msg-1", restored.Context.MessageId);
        Assert.Equal("corr-1", restored.Context.CorrelationId);
        Assert.Equal("cause-1", restored.Context.CausationId);
        Assert.Equal("user-1", restored.Context.UserId);
        Assert.Equal("tenant-1", restored.Context.TenantId);
        Assert.Equal(enqueuedAt, restored.Context.EnqueuedAt);
        Assert.Equal("InMemory", restored.Context.TransportType);
        Assert.Equal("east", restored.Context.Headers["region"]);
        Assert.Equal(context.Headers[MessageHeaders.TraceParent], restored.Context.Headers[MessageHeaders.TraceParent]);
        Assert.Equal("Order:1", restored.Context.Headers[MessageHeaders.OutboxStreamId]);
        Assert.False(restored.Context.Headers.ContainsKey(MessageHeaders.UserId));
    }

    [Fact]
    public void Read_rejects_a_bad_enqueued_at()
    {
        var headers = new Dictionary<string, string>
        {
            [MessageHeaders.EnqueuedAt] = "yesterday",
        };

        var ex = Assert.Throws<InvalidOperationException>(() => MessageEnvelopeHeaders.Read(headers));
        Assert.Contains(MessageHeaders.EnqueuedAt, ex.Message, StringComparison.Ordinal);
    }
}
