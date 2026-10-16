using Amazon.SQS.Model;

namespace LawnDart.Messaging.AwsSqs.Tests;

public class AwsSqsNameAndOptionsTests
{
    [Fact]
    public void Names_start_with_the_subscription()
    {
        Assert.Equal("orders", AwsSqsNames.Topic("orders"));
        Assert.Equal("billing--orders", AwsSqsNames.Queue("orders", "billing"));
        Assert.Equal("billing--orders--dlq", AwsSqsNames.DeadLetterQueue("orders", "billing"));
    }

    [Fact]
    public void A_topic_name_with_a_period_is_rejected()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => AwsSqsNames.Topic("messaging.conformance-ping"));
        Assert.Contains("period", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void A_topic_name_over_256_characters_is_rejected()
    {
        var name = new string('a', 257);
        var ex = Assert.Throws<InvalidOperationException>(() => AwsSqsNames.Topic(name));
        Assert.Contains("256", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_queue_name_over_80_characters_is_rejected()
    {
        var name = new string('a', 81);
        var ex = Assert.Throws<InvalidOperationException>(() => AwsSqsNames.RequireQueue(name));
        Assert.Contains("80", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_dead_letter_name_that_does_not_fit_is_rejected()
    {
        var topic = new string('t', 70);
        var ex = Assert.Throws<InvalidOperationException>(() => AwsSqsNames.Queue(topic, "billing"));
        Assert.Contains("dead-letter", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Fifo_topics_and_queues_are_rejected()
    {
        var topic = Assert.Throws<InvalidOperationException>(() => AwsSqsNames.Topic("orders.fifo"));
        Assert.Contains("FIFO", topic.Message, StringComparison.Ordinal);
        Assert.Contains("not supported", topic.Message, StringComparison.OrdinalIgnoreCase);

        var queue = Assert.Throws<InvalidOperationException>(() => AwsSqsNames.RequireQueue("orders.fifo"));
        Assert.Contains("FIFO", queue.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Retry_delay_defaults_to_five_seconds_and_rejects_more_than_fifteen_minutes()
    {
        var options = new AwsSqsTransportOptions();
        Assert.Equal(TimeSpan.FromSeconds(5), options.RetryDelay);
        Assert.Equal(TimeSpan.FromSeconds(30), options.OperationTimeout);
        Assert.Equal(TimeSpan.FromSeconds(20), options.LongPollWait);
        Assert.Equal(TimeSpan.FromSeconds(30), options.VisibilityTimeout);
        Assert.Equal(12, options.MaxHandlerDeferrals);
        Assert.False(options.ReplaceRouteRule);
        Assert.False(options.DeadLetterFailedMessages);
        Assert.Equal(AwsSqsUnroutablePublish.Warn, options.UnroutablePublish);

        options.EnsureValid();

        var over = new AwsSqsTransportOptions { RetryDelay = TimeSpan.FromMinutes(15) + TimeSpan.FromTicks(1) };
        var error = Assert.Throws<InvalidOperationException>(over.EnsureValid);
        Assert.Contains("15 minutes", error.Message, StringComparison.Ordinal);

        var max = new AwsSqsTransportOptions { RetryDelay = TimeSpan.FromMinutes(15) };
        max.EnsureValid();
    }

    [Fact]
    public void Delay_seconds_round_up_and_stop_at_900()
    {
        Assert.Equal(1, AwsSqsNames.DelaySeconds(TimeSpan.FromMilliseconds(200)));
        Assert.Equal(5, AwsSqsNames.DelaySeconds(TimeSpan.FromSeconds(5)));
        Assert.Equal(900, AwsSqsNames.DelaySeconds(TimeSpan.FromMinutes(15)));
    }

    [Fact]
    public void Long_poll_and_visibility_ranges_are_enforced()
    {
        var longPoll = new AwsSqsTransportOptions { LongPollWait = TimeSpan.FromSeconds(21) };
        var longPollError = Assert.Throws<InvalidOperationException>(longPoll.EnsureValid);
        Assert.Contains("LongPollWait", longPollError.Message, StringComparison.Ordinal);

        var zero = new AwsSqsTransportOptions { LongPollWait = TimeSpan.Zero };
        zero.EnsureValid();

        var visibility = new AwsSqsTransportOptions { VisibilityTimeout = TimeSpan.Zero };
        var visibilityError = Assert.Throws<InvalidOperationException>(visibility.EnsureValid);
        Assert.Contains("VisibilityTimeout", visibilityError.Message, StringComparison.Ordinal);

        var max = new AwsSqsTransportOptions { VisibilityTimeout = TimeSpan.FromHours(12) };
        max.EnsureValid();
    }

    [Fact]
    public void Service_url_must_be_http_and_region_must_be_a_code()
    {
        var url = new AwsSqsTransportOptions { ServiceUrl = "ftp://localhost" };
        var urlError = Assert.Throws<InvalidOperationException>(url.EnsureValid);
        Assert.Contains("ServiceUrl", urlError.Message, StringComparison.Ordinal);

        var local = new AwsSqsTransportOptions { ServiceUrl = "http://localhost:4566" };
        local.EnsureValid();

        var region = new AwsSqsTransportOptions { Region = "https://aws" };
        var regionError = Assert.Throws<InvalidOperationException>(region.EnsureValid);
        Assert.Contains("Region", regionError.Message, StringComparison.Ordinal);

        var ok = new AwsSqsTransportOptions { Region = "us-east-1" };
        ok.EnsureValid();

        new AwsSqsTransportOptions().EnsureValid();
    }

    [Fact]
    public void Filter_merge_keeps_types_already_on_the_policy()
    {
        var current = AwsSqsFilterPolicy.Build(["placed"]);
        var merged = AwsSqsFilterPolicy.Merge(current, ["shipped"], replace: false);
        Assert.Contains("placed", merged, StringComparison.Ordinal);
        Assert.Contains("shipped", merged, StringComparison.Ordinal);
        Assert.True(AwsSqsFilterPolicy.AllowsMissingEventType(merged));
        Assert.True(AwsSqsFilterPolicy.Covers(merged, ["placed", "shipped"], replace: false));
    }

    [Fact]
    public void Replace_drops_a_type_and_keeps_the_missing_attribute_match()
    {
        var current = AwsSqsFilterPolicy.Build(["placed", "shipped"]);
        var replaced = AwsSqsFilterPolicy.Merge(current, ["placed"], replace: true);
        var types = AwsSqsFilterPolicy.ParseTypes(replaced);
        Assert.Equal(["placed"], types);
        Assert.True(AwsSqsFilterPolicy.AllowsMissingEventType(replaced));
        Assert.False(AwsSqsFilterPolicy.Covers(replaced, ["placed", "shipped"], replace: false));
    }

    [Fact]
    public void Publish_attributes_pack_headers_and_stay_under_the_limit()
    {
        var attributes = AwsSqsHeaders.ForPublish(
            new MessageContext
            {
                MessageId = "msg-7",
                UserId = "user-7",
                TenantId = "tenant-7",
                Headers = new Dictionary<string, string> { ["region"] = "east" },
            },
            "order-placed");

        Assert.True(attributes.Count <= AwsSqsHeaders.MaxAttributes);
        Assert.Equal("order-placed", attributes[AwsSqsHeaders.EventType].StringValue);
        Assert.Contains("user-7", attributes[AwsSqsHeaders.Envelope].StringValue, StringComparison.Ordinal);
        Assert.Contains("tenant-7", attributes[AwsSqsHeaders.Envelope].StringValue, StringComparison.Ordinal);
        Assert.Contains("east", attributes[AwsSqsHeaders.Envelope].StringValue, StringComparison.Ordinal);

        var envelope = MessageEnvelopeHeaders.Read(AwsSqsHeaders.ToEnvelope(attributes), "AwsSqs");
        Assert.Equal("user-7", envelope.Context.UserId);
        Assert.Equal("tenant-7", envelope.Context.TenantId);
        Assert.Equal("east", envelope.Context.Headers["region"]);
        Assert.Equal("msg-7", envelope.Context.MessageId);
    }

    [Fact]
    public void Retry_attributes_record_the_attempt()
    {
        var published = AwsSqsHeaders.ForPublish(new MessageContext { MessageId = "msg-1" }, "order-placed");
        var retried = AwsSqsHeaders.ForRetry(published, "order-placed", completedAttempts: 2, deferrals: null);
        Assert.Equal(2, AwsSqsHeaders.CompletedAttempts(retried));
        Assert.Equal("order-placed", AwsSqsHeaders.EventTypeName(retried));
        Assert.True(retried.Count <= AwsSqsHeaders.MaxAttributes);
    }

    [Fact]
    public void A_full_attribute_set_without_the_envelope_cannot_take_another_count()
    {
        var full = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        for (var i = 0; i < AwsSqsHeaders.MaxAttributes; i++)
        {
            full["extra-" + i] = new MessageAttributeValue { DataType = "String", StringValue = "x" };
        }

        var error = Assert.Throws<InvalidOperationException>(() =>
            AwsSqsHeaders.ForRetry(full, "order-placed", completedAttempts: 3, deferrals: 1));
        Assert.Contains("10", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Attempt_folds_into_an_existing_envelope_when_the_set_is_full()
    {
        var full = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal)
        {
            [AwsSqsHeaders.Envelope] = new MessageAttributeValue { DataType = "String", StringValue = "{}" },
        };
        for (var i = 0; i < AwsSqsHeaders.MaxAttributes - 1; i++)
            full["extra-" + i] = new MessageAttributeValue { DataType = "String", StringValue = "x" };

        var retried = AwsSqsHeaders.ForRetry(full, "order-placed", completedAttempts: 3, deferrals: 1);
        Assert.Equal(AwsSqsHeaders.MaxAttributes, retried.Count);
        Assert.False(retried.ContainsKey(AwsSqsHeaders.Attempt));
        Assert.Equal(3, AwsSqsHeaders.CompletedAttempts(retried));
        Assert.Equal(1, AwsSqsHeaders.DeferralCount(retried));
        Assert.Equal("order-placed", AwsSqsHeaders.EventTypeName(retried));
    }

    [Fact]
    public void Receive_count_defaults_to_ten_and_stays_within_1_to_1000()
    {
        Assert.Equal(10, new AwsSqsTransportOptions().MaxReceiveCount);
        new AwsSqsTransportOptions { MaxReceiveCount = 1000 }.EnsureValid();

        var low = new AwsSqsTransportOptions { MaxReceiveCount = 0 };
        var error = Assert.Throws<InvalidOperationException>(low.EnsureValid);
        Assert.Contains("MaxReceiveCount", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_filter_with_more_than_150_values_is_rejected()
    {
        var names = new string[AwsSqsFilterPolicy.MaxValues];
        for (var i = 0; i < names.Length; i++)
            names[i] = "type-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);

        var error = Assert.Throws<InvalidOperationException>(() => AwsSqsFilterPolicy.Build(names));
        Assert.Contains("150", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_message_over_256_kib_is_rejected_before_it_is_sent()
    {
        var body = new string('x', AwsSqsTransportOptions.MaxMessageBytes + 1);
        var error = Assert.Throws<AwsSqsMessageTooLargeException>(() =>
            AwsSqsMessageSize.Require(body, new Dictionary<string, MessageAttributeValue>(), "publish"));
        Assert.Contains("publish", error.Message, StringComparison.Ordinal);
        Assert.True(AwsSqsMessageSize.IsTooLarge(error));

        AwsSqsMessageSize.Require("ok", new Dictionary<string, MessageAttributeValue>(), "publish");
    }
}
