using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using LawnDart.EventStore;
using LawnDart.Messaging.Telemetry;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.RabbitMQ.Tests;

[Collection(RabbitMqCollection.Name)]
[Trait("Category", "Integration")]
public sealed class RabbitMqPublishAndRetryTests
{
    private readonly RabbitMqContainerFixture _container;

    public RabbitMqPublishAndRetryTests(RabbitMqContainerFixture container) => _container = container;

    [Fact]
    public async Task Publish_only_declares_the_exchange_and_warns_when_nothing_is_bound()
    {
        var durations = 0L;
        var errors = 0L;
        string? outcome = null;
        using var meters = new MeterListener();
        meters.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == MessagingTelemetry.MeterName &&
                instrument.Name is "messaging.publish.duration" or "messaging.publish.errors")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meters.SetMeasurementEventCallback<double>((instrument, _, tags, _) =>
        {
            if (instrument.Name == "messaging.publish.duration" &&
                HasTag(tags, "messaging.message_type", "messaging.rabbitmq.publish-only"))
            {
                Interlocked.Increment(ref durations);
            }
        });
        meters.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            if (instrument.Name != "messaging.publish.errors" ||
                !HasTag(tags, "messaging.message_type", "messaging.rabbitmq.publish-only"))
            {
                return;
            }

            Interlocked.Add(ref errors, value);
            foreach (var tag in tags)
            {
                if (tag.Key == "messaging.publish.outcome")
                    outcome = tag.Value as string;
            }
        });
        meters.Start();

        string? activityOutcome = null;
        ActivityStatusCode? activityStatus = null;
        using var activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessagingTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.OperationName == "Messaging.Publish" &&
                    activity.GetTagItem("messaging.message_type") as string == "messaging.rabbitmq.publish-only")
                {
                    activityOutcome = activity.GetTagItem("messaging.publish.outcome") as string;
                    activityStatus = activity.Status;
                }
            },
        };
        ActivitySource.AddActivityListener(activities);

        var logger = new WarningLogger();
        await using var fixture = RabbitMqTransportFixture.Create(
            _container.AmqpUri,
            configure: null,
            preHandler: null,
            logger);

        await fixture.Transport.PublishAsync(
            new PublishOnlyPing(Guid.NewGuid(), DateTime.UtcNow, "publish-only"),
            new MessageContext { MessageId = "pub-1" });

        var warning = Assert.Single(logger.Warnings);
        Assert.Contains("pub-1", warning, StringComparison.Ordinal);
        Assert.Contains("not queued", warning, StringComparison.Ordinal);

        var declared = "messaging.rabbitmq.publish-only";
        var subscription = fixture.Options.SubscriptionName;
        await using var connection = await OpenAsync();
        await using var channel = await connection.CreateChannelAsync();

        var missingQueue = await Record.ExceptionAsync(() => channel.QueueDeclareAsync(
            RabbitMqNames.Queue(subscription, RabbitMqQueueLayout.Subscription, declared),
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            passive: true));
        Assert.NotNull(missingQueue);

        await using var exchangeChannel = await connection.CreateChannelAsync();
        await exchangeChannel.ExchangeDeclareAsync(
            RabbitMqNames.Exchange(declared),
            ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            passive: true);

        var missingRetry = await Record.ExceptionAsync(() => exchangeChannel.QueueDeclareAsync(
            RabbitMqNames.RetryQueue(subscription),
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            passive: true));
        Assert.NotNull(missingRetry);
        Assert.Equal(0, Volatile.Read(ref durations));
        Assert.Equal(1, Volatile.Read(ref errors));
        Assert.Equal("unroutable", outcome);
        Assert.Equal("unroutable", activityOutcome);
        Assert.Equal(ActivityStatusCode.Error, activityStatus);
    }

    [Fact]
    public async Task A_cancelled_delivery_waits_on_the_retry_queue_without_incrementing_the_attempt()
    {
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.RetryDelay = TimeSpan.FromSeconds(3);
        }, preHandler: null);

        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = fixture.Transport.SubscribeAsync<ConformancePing>(async (_, _, token) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
        }, cts.Token);

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "cancel-delay"),
            MessageContext.New());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await cts.CancelAsync();
        await subscription.WaitAsync(TimeSpan.FromSeconds(10));

        var retryQueue = RabbitMqNames.RetryQueue(fixture.Options.SubscriptionName);
        var mainQueue = RabbitMqNames.Queue(
            fixture.Options.SubscriptionName,
            RabbitMqQueueLayout.Subscription,
            "messaging.conformance-ping");
        await WaitUntilAsync(async () => await CountAsync(retryQueue) >= 1);
        Assert.Equal(0u, await CountAsync(mainQueue));

        await using var connection = await OpenAsync();
        await using var channel = await connection.CreateChannelAsync();
        var got = await channel.BasicGetAsync(retryQueue, autoAck: true);
        Assert.NotNull(got);
        var headers = RabbitMqHeaders.Copy(got.BasicProperties.Headers);
        Assert.NotNull(headers);
        Assert.True(headers.ContainsKey(RabbitMqHeaders.Attempt));
        Assert.Equal(0, RabbitMqHeaders.CompletedAttempts(headers));
    }

    private static bool HasTag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key, string expected)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == key && tag.Value as string == expected)
                return true;
        }

        return false;
    }

    [Fact]
    public async Task Unroutable_publish_throws_when_that_option_is_set()
    {
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.UnroutablePublish = RabbitMqUnroutablePublish.Throw;
        }, preHandler: null);

        var ex = await Assert.ThrowsAsync<PublishReturnException>(() => fixture.Transport.PublishAsync(
            new UnroutablePing(Guid.NewGuid(), DateTime.UtcNow, "throw"),
            MessageContext.New()));
        Assert.Contains("NO_ROUTE", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_failed_delivery_waits_on_the_retry_queue_before_the_next_attempt()
    {
        var delay = TimeSpan.FromMilliseconds(800);
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.RetryDelay = delay;
            options.MaxDeliveryAttempts = 3;
        }, preHandler: null);

        var stamps = new List<DateTimeOffset>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await fixture.Transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            stamps.Add(DateTimeOffset.UtcNow);
            if (stamps.Count < 2)
                throw new InvalidOperationException("again");
            done.TrySetResult();
            return Task.CompletedTask;
        });

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "delayed"),
            MessageContext.New());
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(stamps[1] - stamps[0] >= delay - TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public async Task A_delivery_with_no_handler_waits_on_the_retry_queue()
    {
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.RetryDelay = TimeSpan.FromSeconds(3);
        }, preHandler: null);
        var transport = Assert.IsType<RabbitMqMessageTransport>(fixture.Transport);

        var calls = 0;
        await transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.CompletedTask;
        });
        transport.RemoveHandlers<ConformancePing>();

        await transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "idle"),
            MessageContext.New());

        var retryQueue = RabbitMqNames.RetryQueue(fixture.Options.SubscriptionName);
        await WaitUntilAsync(async () => await CountAsync(retryQueue) >= 1);
        await Task.Delay(300);

        Assert.Equal(0, Volatile.Read(ref calls));
        Assert.Equal(1u, await CountAsync(retryQueue));
    }

    private async Task<IConnection> OpenAsync()
    {
        var factory = new ConnectionFactory { Uri = new Uri(_container.AmqpUri) };
        return await factory.CreateConnectionAsync();
    }

    private async Task<uint> CountAsync(string queue)
    {
        await using var connection = await OpenAsync();
        await using var channel = await connection.CreateChannelAsync();
        return await channel.MessageCountAsync(queue);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met before the timeout.");
            await Task.Delay(20);
        }
    }

    [EventTypeName("messaging.rabbitmq.publish-only")]
    private sealed record PublishOnlyPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName("messaging.rabbitmq.unroutable")]
    private sealed record UnroutablePing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    private sealed class WarningLogger : ILogger<RabbitMqMessageTransport>
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
