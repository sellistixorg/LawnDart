namespace LawnDart.Messaging.RabbitMQ.Tests;

public class RabbitMqTransportOptionsTests
{
    [Fact]
    public void Names_use_the_declared_type_and_subscription()
    {
        Assert.Equal("order.placed", RabbitMqNames.Exchange("order.placed"));
        Assert.Equal("order.placed", RabbitMqNames.RoutingKey("order.placed"));
        Assert.Equal("order.placed.billing", RabbitMqNames.Queue("order.placed", "billing"));
        Assert.Equal("order.placed.dlx", RabbitMqNames.DeadLetterExchange("order.placed"));
        Assert.Equal("order.placed.billing", RabbitMqNames.DeadLetterRoutingKey("order.placed", "billing"));
        Assert.Equal("order.placed.billing.dlq", RabbitMqNames.DeadLetterQueue("order.placed", "billing"));
    }

    [Fact]
    public void An_amqp_uri_is_accepted()
    {
        var options = new RabbitMqTransportOptions { Uri = "amqp://rabbitmq:rabbitmq@localhost:5672/" };
        options.EnsureValid();
    }

    [Fact]
    public void A_non_amqp_uri_is_rejected()
    {
        var options = new RabbitMqTransportOptions { Uri = "http://localhost/" };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("Uri", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Prefetch_above_the_qos_limit_is_rejected()
    {
        var options = new RabbitMqTransportOptions { Prefetch = ushort.MaxValue + 1 };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("Prefetch", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_blank_host_is_rejected_when_no_uri_is_set()
    {
        var options = new RabbitMqTransportOptions { HostName = " " };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("HostName", ex.Message, StringComparison.Ordinal);
    }
}
