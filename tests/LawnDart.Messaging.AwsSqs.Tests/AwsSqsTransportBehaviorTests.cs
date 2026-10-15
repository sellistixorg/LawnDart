using System.Diagnostics;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.Telemetry;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.AwsSqs.Tests;

[Collection(AwsSqsCollection.Name)]
[Trait("Category", "Integration")]
public sealed class AwsSqsTransportBehaviorTests
{
    private readonly AwsSqsContainerFixture _container;

    public AwsSqsTransportBehaviorTests(AwsSqsContainerFixture container) => _container = container;

    [Fact]
    public async Task A_failed_delivery_waits_before_the_next_attempt()
    {
        var delay = TimeSpan.FromSeconds(2);
        await using var fixture = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                var sqs = Assert.IsType<AwsSqsTransportOptions>(options);
                sqs.RetryDelay = delay;
                options.MaxDeliveryAttempts = 3;
            },
            preHandler: null);

        var stamps = new List<DateTimeOffset>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await fixture.Transport.SubscribeAsync<RetryDelayPing>((_, _, _) =>
        {
            stamps.Add(DateTimeOffset.UtcNow);
            if (stamps.Count < 2)
                throw new InvalidOperationException("again");
            done.TrySetResult();
            return Task.CompletedTask;
        });

        await fixture.Transport.PublishAsync(
            new RetryDelayPing(Guid.NewGuid(), DateTime.UtcNow, "delayed"),
            MessageContext.New());
        await done.Task.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.True(stamps[1] - stamps[0] >= delay - TimeSpan.FromMilliseconds(250));
    }

    [Fact]
    public async Task A_retry_is_delivered_only_to_the_subscription_that_failed()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        await using var first = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                options.SubscriptionName = "a-" + suffix;
                options.MaxDeliveryAttempts = 3;
            },
            preHandler: null);
        await using var second = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options => options.SubscriptionName = "b-" + suffix,
            preHandler: null);

        var firstCalls = 0;
        var secondCalls = 0;
        var firstDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await first.Transport.SubscribeAsync<TwoSubPing>((_, _, _) =>
        {
            if (Interlocked.Increment(ref firstCalls) == 1)
                throw new InvalidOperationException("again");
            firstDone.TrySetResult();
            return Task.CompletedTask;
        });
        await second.Transport.SubscribeAsync<TwoSubPing>((_, _, _) =>
        {
            Interlocked.Increment(ref secondCalls);
            return Task.CompletedTask;
        });

        await first.Transport.PublishAsync(
            new TwoSubPing(Guid.NewGuid(), DateTime.UtcNow, "one-subscriber"),
            MessageContext.New());
        await firstDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await Task.Delay(800);

        Assert.Equal(2, Volatile.Read(ref firstCalls));
        Assert.Equal(1, Volatile.Read(ref secondCalls));
    }

    [Fact]
    public async Task A_grouped_topic_delivers_each_type_only_to_its_subscriber_and_retries_only_the_failure()
    {
        var topic = "grp-" + Guid.NewGuid().ToString("N")[..8];
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var mapper = new GroupTopicMapper(new Dictionary<string, string>
        {
            [PlacedName] = topic,
            [ShippedName] = topic,
        });

        await using var placed = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                options.SubscriptionName = "placed-" + suffix;
                options.MaxDeliveryAttempts = 3;
                options.TopicMapper = mapper;
            },
            preHandler: null);
        await using var shipped = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                options.SubscriptionName = "shipped-" + suffix;
                options.TopicMapper = mapper;
            },
            preHandler: null);

        var placedTexts = new List<string>();
        var shippedTexts = new List<string>();
        var placedDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var shippedDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await placed.Transport.SubscribeAsync<PlacedPing>((message, _, _) =>
        {
            lock (placedTexts)
            {
                placedTexts.Add(message.Text);
                if (placedTexts.Count == 1)
                    throw new InvalidOperationException("again");
            }

            placedDone.TrySetResult();
            return Task.CompletedTask;
        });
        await shipped.Transport.SubscribeAsync<ShippedPing>((message, _, _) =>
        {
            lock (shippedTexts)
                shippedTexts.Add(message.Text);
            shippedDone.TrySetResult();
            return Task.CompletedTask;
        });

        await placed.Transport.PublishAsync(new PlacedPing(Guid.NewGuid(), DateTime.UtcNow, "placed"), MessageContext.New());
        await shipped.Transport.PublishAsync(new ShippedPing(Guid.NewGuid(), DateTime.UtcNow, "shipped"), MessageContext.New());

        await placedDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await shippedDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await Task.Delay(800);

        Assert.Equal(["placed", "placed"], Snapshot(placedTexts));
        Assert.Equal(["shipped"], Snapshot(shippedTexts));

        using var sns = Sns();
        var topicArn = (await sns.CreateTopicAsync(topic)).TopicArn;
        var placedPolicy = await FilterAsync(sns, topicArn, QueueArn(topic, "placed-" + suffix));
        var shippedPolicy = await FilterAsync(sns, topicArn, QueueArn(topic, "shipped-" + suffix));
        Assert.Contains(PlacedName, AwsSqsFilterPolicy.ParseTypes(placedPolicy));
        Assert.DoesNotContain(ShippedName, AwsSqsFilterPolicy.ParseTypes(placedPolicy));
        Assert.Contains(ShippedName, AwsSqsFilterPolicy.ParseTypes(shippedPolicy));
        Assert.DoesNotContain(PlacedName, AwsSqsFilterPolicy.ParseTypes(shippedPolicy));
        Assert.True(AwsSqsFilterPolicy.AllowsMissingEventType(placedPolicy));
    }

    [Fact]
    public async Task One_subscription_on_a_grouped_topic_dispatches_each_type_to_its_handler()
    {
        var topic = "one-" + Guid.NewGuid().ToString("N")[..8];
        await using var fixture = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                options.MaxDeliveryAttempts = 3;
                options.TopicMapper = new GroupTopicMapper(new Dictionary<string, string>
                {
                    [PlacedName] = topic,
                    [ShippedName] = topic,
                });
            },
            preHandler: null);

        var placedTexts = new List<string>();
        var shippedTexts = new List<string>();
        var placedDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var shippedDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await fixture.Transport.SubscribeAsync<PlacedPing>((message, _, _) =>
        {
            lock (placedTexts)
            {
                placedTexts.Add(message.Text);
                if (placedTexts.Count == 1)
                    throw new InvalidOperationException("again");
            }

            placedDone.TrySetResult();
            return Task.CompletedTask;
        });
        await fixture.Transport.SubscribeAsync<ShippedPing>((message, _, _) =>
        {
            lock (shippedTexts)
                shippedTexts.Add(message.Text);
            shippedDone.TrySetResult();
            return Task.CompletedTask;
        });

        await fixture.Transport.PublishAsync(new PlacedPing(Guid.NewGuid(), DateTime.UtcNow, "placed"), MessageContext.New());
        await fixture.Transport.PublishAsync(new ShippedPing(Guid.NewGuid(), DateTime.UtcNow, "shipped"), MessageContext.New());

        await placedDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await shippedDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await Task.Delay(800);

        Assert.Equal(["placed", "placed"], Snapshot(placedTexts));
        Assert.Equal(["shipped"], Snapshot(shippedTexts));
    }

    [Fact]
    public async Task Exhausted_message_lands_on_the_subscription_dead_letter_queue()
    {
        var logger = new WarningLogger();
        await using var fixture = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                options.MaxDeliveryAttempts = 2;
                options.DeadLetterFailedMessages = true;
            },
            preHandler: null,
            logger);

        await fixture.Transport.SubscribeAsync<DeadLetterPing>((_, _, _) =>
            throw new InvalidOperationException("park-me"));

        await fixture.Transport.PublishAsync(
            new DeadLetterPing(Guid.NewGuid(), DateTime.UtcNow, "dead-letter-body"),
            new MessageContext { MessageId = "dlq-1" });

        await WaitUntilAsync(() => fixture.DeadLetters.Count >= 1);
        var parked = Assert.Single(fixture.DeadLetters);
        Assert.Equal(2, parked.Attempts);
        Assert.Equal("dlq-1", parked.Context.MessageId);
        Assert.Contains(logger.Warnings, warning => warning.Contains("Dead-lettered", StringComparison.Ordinal));

        using var sqs = Sqs();
        var topic = Topic(typeof(DeadLetterPing));
        var url = await sqs.GetQueueUrlAsync(AwsSqsNames.DeadLetterQueue(topic, fixture.Options.SubscriptionName));
        var dead = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = url.QueueUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 10,
        });
        var body = Assert.Single(dead.Messages).Body;
        Assert.Contains("dead-letter-body", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_cancelled_delivery_is_not_available_until_the_retry_delay()
    {
        var delay = TimeSpan.FromSeconds(2);
        await using var fixture = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                var sqs = Assert.IsType<AwsSqsTransportOptions>(options);
                sqs.RetryDelay = delay;
            },
            preHandler: null);

        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = fixture.Transport.SubscribeAsync<CancelPing>(async (_, _, token) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
        }, cts.Token);

        await fixture.Transport.PublishAsync(
            new CancelPing(Guid.NewGuid(), DateTime.UtcNow, "cancel-delay"),
            MessageContext.New());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));

        var cancelledAt = DateTimeOffset.UtcNow;
        await cts.CancelAsync();
        await subscription.WaitAsync(TimeSpan.FromSeconds(15));

        using var sqs = Sqs();
        var topic = Topic(typeof(CancelPing));
        var url = await sqs.GetQueueUrlAsync(AwsSqsNames.Queue(topic, fixture.Options.SubscriptionName));
        var early = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = url.QueueUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 0,
            VisibilityTimeout = 1,
        });
        Assert.Empty(early.Messages ?? []);

        var later = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = url.QueueUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 10,
        });
        Assert.NotEmpty(later.Messages ?? []);
        Assert.True(DateTimeOffset.UtcNow - cancelledAt >= delay - TimeSpan.FromMilliseconds(250));
    }

    [Fact]
    public async Task User_tenant_and_custom_headers_arrive_on_the_handler()
    {
        await using var fixture = AwsSqsTransportFixture.Create(_container.ServiceUrl, configure: null, preHandler: null);
        MessageContext? received = null;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await fixture.Transport.SubscribeAsync<HeaderPing>((_, context, _) =>
        {
            received = context;
            done.TrySetResult();
            return Task.CompletedTask;
        });

        await fixture.Transport.PublishAsync(
            new HeaderPing(Guid.NewGuid(), DateTime.UtcNow, "headers"),
            new MessageContext
            {
                MessageId = "msg-headers",
                UserId = "user-headers",
                TenantId = "tenant-headers",
                Headers = new Dictionary<string, string> { ["region"] = "east" },
            });
        await done.Task.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.NotNull(received);
        Assert.Equal("msg-headers", received.MessageId);
        Assert.Equal("user-headers", received.UserId);
        Assert.Equal("tenant-headers", received.TenantId);
        Assert.Equal("east", received.Headers["region"]);
        Assert.Null(received.Headers.GetValueOrDefault("authorization"));
    }

    [Fact]
    public async Task A_raw_message_without_lawndart_attributes_reaches_every_subscription()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        await using var first = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options => options.SubscriptionName = "raw-a-" + suffix,
            preHandler: null);
        await using var second = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options => options.SubscriptionName = "raw-b-" + suffix,
            preHandler: null);

        var firstDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        RawFanoutPing? firstMessage = null;
        RawFanoutPing? secondMessage = null;
        await first.Transport.SubscribeAsync<RawFanoutPing>((message, _, _) =>
        {
            firstMessage = message;
            firstDone.TrySetResult();
            return Task.CompletedTask;
        });
        await second.Transport.SubscribeAsync<RawFanoutPing>((message, _, _) =>
        {
            secondMessage = message;
            secondDone.TrySetResult();
            return Task.CompletedTask;
        });

        var sent = new RawFanoutPing(Guid.NewGuid(), DateTime.UtcNow, "from-outside");
        using var sns = Sns();
        var topicArn = (await sns.CreateTopicAsync(Topic(typeof(RawFanoutPing)))).TopicArn;
        await sns.PublishAsync(new PublishRequest
        {
            TopicArn = topicArn,
            Message = JsonSerializer.Serialize(sent, MessagingJson.Options),
            MessageAttributes = new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>
            {
                ["publisher"] = new() { DataType = "String", StringValue = "external" },
            },
        });

        await firstDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await secondDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.Equal(sent.Text, firstMessage?.Text);
        Assert.Equal(sent.Id, firstMessage?.Id);
        Assert.Equal(sent.Text, secondMessage?.Text);
        Assert.Equal(sent.Id, secondMessage?.Id);
    }

    [Fact]
    public async Task Two_hosts_starting_the_same_subscription_both_finish()
    {
        var topic = "race-" + Guid.NewGuid().ToString("N")[..8];
        var subscription = "race-" + Guid.NewGuid().ToString("N")[..8];
        var mapper = new GroupTopicMapper(new Dictionary<string, string> { [PlacedName] = topic });
        var planned = new MessageSubscription[] { new(typeof(PlacedPing)) };

        await using var first = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                options.SubscriptionName = subscription;
                options.TopicMapper = mapper;
            },
            preHandler: null,
            subscriptions: planned);
        await using var second = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                options.SubscriptionName = subscription;
                options.TopicMapper = mapper;
            },
            preHandler: null,
            subscriptions: planned);

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var left = Task.Run(async () =>
        {
            await gate.Task;
            await first.Transport.SubscribeAsync<PlacedPing>((_, _, _) => Task.CompletedTask);
        });
        var right = Task.Run(async () =>
        {
            await gate.Task;
            await second.Transport.SubscribeAsync<PlacedPing>((_, _, _) => Task.CompletedTask);
        });
        gate.TrySetResult();
        await Task.WhenAll(left, right);

        using var sns = Sns();
        var topicArn = (await sns.CreateTopicAsync(topic)).TopicArn;
        var matches = await SubscriptionsAsync(sns, topicArn, QueueArn(topic, subscription));
        var only = Assert.Single(matches);
        var attributes = await sns.GetSubscriptionAttributesAsync(only);
        Assert.Equal("true", attributes.Attributes["RawMessageDelivery"]);
        Assert.Contains(PlacedName, AwsSqsFilterPolicy.ParseTypes(attributes.Attributes["FilterPolicy"]));
        Assert.True(AwsSqsFilterPolicy.AllowsMissingEventType(attributes.Attributes["FilterPolicy"]));
    }

    [Fact]
    public async Task A_handler_slower_than_the_visibility_timeout_is_delivered_once()
    {
        await using var fixture = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                var sqs = Assert.IsType<AwsSqsTransportOptions>(options);
                sqs.VisibilityTimeout = TimeSpan.FromSeconds(2);
                options.Concurrency = 2;
            },
            preHandler: null);

        var calls = 0;
        var inFlight = 0;
        var maxInFlight = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await fixture.Transport.SubscribeAsync<SlowPing>(async (_, _, _) =>
        {
            var now = Interlocked.Increment(ref inFlight);
            int observed;
            do
            {
                observed = Volatile.Read(ref maxInFlight);
                if (now <= observed)
                    break;
            }
            while (Interlocked.CompareExchange(ref maxInFlight, now, observed) != observed);

            Interlocked.Increment(ref calls);
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
            finally
            {
                Interlocked.Decrement(ref inFlight);
            }

            done.TrySetResult();
        });

        await fixture.Transport.PublishAsync(
            new SlowPing(Guid.NewGuid(), DateTime.UtcNow, "slow"),
            MessageContext.New());
        await done.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await Task.Delay(TimeSpan.FromSeconds(3));

        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.Equal(1, Volatile.Read(ref maxInFlight));
    }

    [Fact]
    public async Task Two_hosts_with_different_types_keep_both_types_on_the_filter()
    {
        var topic = "mix-" + Guid.NewGuid().ToString("N")[..8];
        var subscription = "mix-" + Guid.NewGuid().ToString("N")[..8];
        var mapper = new GroupTopicMapper(new Dictionary<string, string>
        {
            [PlacedName] = topic,
            [ShippedName] = topic,
        });

        await using var placed = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                options.SubscriptionName = subscription;
                options.TopicMapper = mapper;
            },
            preHandler: null,
            subscriptions: [new MessageSubscription(typeof(PlacedPing))]);
        await using var shipped = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                options.SubscriptionName = subscription;
                options.TopicMapper = mapper;
            },
            preHandler: null,
            subscriptions: [new MessageSubscription(typeof(ShippedPing))]);

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var left = Task.Run(async () =>
        {
            await gate.Task;
            await placed.Transport.SubscribeAsync<PlacedPing>((_, _, _) => Task.CompletedTask);
        });
        var right = Task.Run(async () =>
        {
            await gate.Task;
            await shipped.Transport.SubscribeAsync<ShippedPing>((_, _, _) => Task.CompletedTask);
        });
        gate.TrySetResult();
        await Task.WhenAll(left, right);

        using var sns = Sns();
        var topicArn = (await sns.CreateTopicAsync(topic)).TopicArn;
        var policy = await FilterAsync(sns, topicArn, QueueArn(topic, subscription));
        var types = AwsSqsFilterPolicy.ParseTypes(policy);
        Assert.Contains(PlacedName, types);
        Assert.Contains(ShippedName, types);
    }

    [Fact]
    public async Task An_unroutable_publish_is_not_a_successful_duration()
    {
        await using var fixture = AwsSqsTransportFixture.Create(
            _container.ServiceUrl,
            options =>
            {
                var sqs = Assert.IsType<AwsSqsTransportOptions>(options);
                sqs.UnroutablePublish = AwsSqsUnroutablePublish.Throw;
            },
            preHandler: null);

        var errors = 0L;
        var durations = 0L;
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == "LawnDart.Messaging" &&
                instrument.Name is "messaging.publish.errors" or "messaging.publish.duration")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == "messaging.publish.errors")
                Interlocked.Add(ref errors, value);
        });
        listener.SetMeasurementEventCallback<double>((instrument, _, _, _) =>
        {
            if (instrument.Name == "messaging.publish.duration")
                Interlocked.Increment(ref durations);
        });
        listener.Start();

        using var activities = StartListener();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Transport.PublishAsync(
                new UnroutablePing(Guid.NewGuid(), DateTime.UtcNow, "nowhere"),
                MessageContext.New()));

        Assert.True(Volatile.Read(ref errors) >= 1);
        Assert.Equal(0, Volatile.Read(ref durations));
        activities.Dispose();
    }

    private static string Topic(Type messageType)
        => MessageTypeNames.Require(messageType).Replace('.', '-');

    private string QueueArn(string topic, string subscription)
    {
        using var sqs = Sqs();
        var name = AwsSqsNames.Queue(topic, subscription);
        var url = sqs.GetQueueUrlAsync(name).GetAwaiter().GetResult().QueueUrl;
        var attributes = sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = url,
            AttributeNames = ["QueueArn"],
        }).GetAwaiter().GetResult();
        return attributes.Attributes["QueueArn"];
    }

    private static async Task<string> FilterAsync(IAmazonSimpleNotificationService sns, string topicArn, string queueArn)
    {
        var matches = await SubscriptionsAsync(sns, topicArn, queueArn);
        var arn = Assert.Single(matches);
        var attributes = await sns.GetSubscriptionAttributesAsync(arn);
        return attributes.Attributes["FilterPolicy"];
    }

    private static async Task<List<string>> SubscriptionsAsync(
        IAmazonSimpleNotificationService sns,
        string topicArn,
        string queueArn)
    {
        var found = new List<string>();
        string? token = null;
        do
        {
            var page = await sns.ListSubscriptionsByTopicAsync(new ListSubscriptionsByTopicRequest
            {
                TopicArn = topicArn,
                NextToken = token,
            });
            foreach (var subscription in page.Subscriptions)
            {
                if (string.Equals(subscription.Endpoint, queueArn, StringComparison.Ordinal) &&
                    !string.Equals(subscription.SubscriptionArn, "PendingConfirmation", StringComparison.Ordinal))
                {
                    found.Add(subscription.SubscriptionArn);
                }
            }

            token = page.NextToken;
        }
        while (!string.IsNullOrEmpty(token));

        return found;
    }

    private IAmazonSQS Sqs()
        => new AmazonSQSClient(
            new BasicAWSCredentials("test", "test"),
            new AmazonSQSConfig { ServiceURL = _container.ServiceUrl, AuthenticationRegion = "us-east-1" });

    private IAmazonSimpleNotificationService Sns()
        => new AmazonSimpleNotificationServiceClient(
            new BasicAWSCredentials("test", "test"),
            new AmazonSimpleNotificationServiceConfig
            {
                ServiceURL = _container.ServiceUrl,
                AuthenticationRegion = "us-east-1",
            });

    private static ActivityListener StartListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessagingTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met before the timeout.");
            await Task.Delay(20);
        }
    }

    private static List<string> Snapshot(List<string> texts)
    {
        lock (texts)
            return [.. texts];
    }

    private const string PlacedName = "awssqs-placed";
    private const string ShippedName = "awssqs-shipped";

    [EventTypeName("awssqs-retry-delay")]
    private sealed record RetryDelayPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName("awssqs-two-subs")]
    private sealed record TwoSubPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName(PlacedName)]
    private sealed record PlacedPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName(ShippedName)]
    private sealed record ShippedPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName("awssqs-dead-letter")]
    private sealed record DeadLetterPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName("awssqs-cancel")]
    private sealed record CancelPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName("awssqs-headers")]
    private sealed record HeaderPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName("awssqs-raw-fanout")]
    private sealed record RawFanoutPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName("awssqs-unroutable")]
    private sealed record UnroutablePing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName("awssqs-slow-handler")]
    private sealed record SlowPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    private sealed class WarningLogger : ILogger<AwsSqsMessageTransport>
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }
}
