namespace LawnDart.Messaging.RabbitMQ.Tests;

public class RabbitMqTransportOptionsTests
{
    [Fact]
    public void Names_start_with_the_subscription_and_a_fixed_suffix()
    {
        Assert.Equal(RabbitMqQueueLayout.Subscription, new RabbitMqTransportOptions().QueueLayout);
        Assert.Equal("order-placed", RabbitMqNames.Exchange("order-placed"));
        Assert.Equal("order-placed", RabbitMqNames.RoutingKey("order-placed"));

        Assert.Equal("billing", RabbitMqNames.Queue("billing", RabbitMqQueueLayout.Subscription, "order-placed"));
        Assert.Equal(
            "billing.order-placed",
            RabbitMqNames.Queue("billing", RabbitMqQueueLayout.EventType, "order-placed"));
        Assert.Equal("billing.retry", RabbitMqNames.RetryQueue("billing"));
        Assert.Equal("billing.retry", RabbitMqNames.RetryExchange("billing"));
        Assert.Equal("billing.delay", RabbitMqNames.DelayExchange("billing"));
        Assert.Equal("billing.dlx", RabbitMqNames.DeadLetterExchange("billing"));
        Assert.Equal("billing.dlq", RabbitMqNames.DeadLetterQueue("billing"));

        Assert.Equal(".retry", RabbitMqNames.RetrySuffix);
        Assert.Equal(".delay", RabbitMqNames.DelaySuffix);
        Assert.Equal(".dlx", RabbitMqNames.DeadLetterExchangeSuffix);
        Assert.Equal(".dlq", RabbitMqNames.DeadLetterQueueSuffix);

        RabbitMqNames.RequireConsumerNames("billing", RabbitMqQueueLayout.Subscription, "order-placed");
        RabbitMqNames.RequireConsumerNames("billing", RabbitMqQueueLayout.EventType, "order-placed");
    }

    [Fact]
    public void A_per_event_queue_that_matches_a_retry_or_dead_letter_name_is_rejected()
    {
        var retry = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqNames.RequireConsumerNames("billing", RabbitMqQueueLayout.EventType, "retry"));
        Assert.Contains("billing.retry", retry.Message, StringComparison.Ordinal);

        var deadLetter = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqNames.RequireConsumerNames("billing", RabbitMqQueueLayout.EventType, "dlq"));
        Assert.Contains("billing.dlq", deadLetter.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_suffix_that_passes_255_bytes_is_rejected()
    {
        var subscription = new string('a', 250);
        RabbitMqNames.RequireQueue(subscription);

        var length = Assert.Throws<InvalidOperationException>(() =>
            RabbitMqNames.RequireConsumerNames(subscription, RabbitMqQueueLayout.Subscription, "order-placed"));
        Assert.Contains("255", length.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void An_unknown_queue_layout_is_rejected()
    {
        var options = new RabbitMqTransportOptions { QueueLayout = (RabbitMqQueueLayout)9 };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("QueueLayout", ex.Message, StringComparison.Ordinal);
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
    public void Concurrency_outside_the_ushort_range_is_rejected()
    {
        var options = new RabbitMqTransportOptions { Concurrency = ushort.MaxValue + 1 };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("Concurrency", ex.Message, StringComparison.Ordinal);
        Assert.Contains("1 to 65535", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Retry_delay_defaults_to_five_seconds_and_must_be_positive()
    {
        Assert.Equal(TimeSpan.FromSeconds(5), new RabbitMqTransportOptions().RetryDelay);
        Assert.Equal(TimeSpan.FromSeconds(30), new RabbitMqTransportOptions().OperationTimeout);
        Assert.Equal(RabbitMqUnroutablePublish.Warn, new RabbitMqTransportOptions().UnroutablePublish);

        var options = new RabbitMqTransportOptions { RetryDelay = TimeSpan.Zero };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("RetryDelay", ex.Message, StringComparison.Ordinal);

        var timeout = new RabbitMqTransportOptions { OperationTimeout = TimeSpan.Zero };
        var timeoutError = Assert.Throws<InvalidOperationException>(timeout.EnsureValid);
        Assert.Contains("OperationTimeout", timeoutError.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Exchange_queue_and_routing_key_names_have_a_255_byte_limit()
    {
        Assert.Equal(255, RabbitMqNames.NameMaxLength);
        RabbitMqNames.RequireExchange(new string('a', 255));

        var longName = new string('a', 256);
        var length = Assert.Throws<InvalidOperationException>(() => RabbitMqNames.RequireQueue(longName));
        Assert.Contains("255", length.Message, StringComparison.Ordinal);

        var reserved = Assert.Throws<InvalidOperationException>(() => RabbitMqNames.RequireExchange("amq.direct"));
        Assert.Contains("reserved", reserved.Message, StringComparison.Ordinal);

        var blank = Assert.Throws<InvalidOperationException>(() => RabbitMqNames.RequireRoutingKey(" "));
        Assert.Contains("required", blank.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_blank_host_is_rejected_when_no_uri_is_set()
    {
        var options = new RabbitMqTransportOptions { HostName = " " };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("HostName", ex.Message, StringComparison.Ordinal);
    }
}
