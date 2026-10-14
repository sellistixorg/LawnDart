using System.Diagnostics;
using System.Text;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.Telemetry;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.AzureServiceBus.Tests;

[Collection(AzureServiceBusCollection.Name)]
[Trait("Category", "Integration")]
public sealed class AzureServiceBusTransportBehaviorTests
{
    private readonly AzureServiceBusContainerFixture _container;

    public AzureServiceBusTransportBehaviorTests(AzureServiceBusContainerFixture container) => _container = container;

    [Fact]
    public async Task Publish_creates_the_topic_and_not_the_subscription()
    {
        await using var fixture = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            configure: null,
            preHandler: null);

        await fixture.Transport.PublishAsync(
            new PublishOnlyPing(Guid.NewGuid(), DateTime.UtcNow, "publish-only"),
            new MessageContext { MessageId = "pub-1" });

        var topic = AzureServiceBusNames.Topic("messaging.servicebus.publish-only");
        var subscription = fixture.Options.SubscriptionName;
        var admin = new ServiceBusAdministrationClient(_container.AdminConnectionString);
        Assert.True(await admin.TopicExistsAsync(topic));
        Assert.False(await admin.SubscriptionExistsAsync(topic, subscription));
    }

    [Fact]
    public async Task A_failed_delivery_waits_before_the_next_attempt()
    {
        var delay = TimeSpan.FromMilliseconds(800);
        await using var fixture = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                var serviceBus = Assert.IsType<AzureServiceBusTransportOptions>(options);
                serviceBus.RetryDelay = delay;
                options.MaxDeliveryAttempts = 3;
            },
            preHandler: null);

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
        await done.Task.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.True(stamps[1] - stamps[0] >= delay - TimeSpan.FromMilliseconds(150));
    }

    [Fact]
    public async Task A_cancelled_delivery_is_not_available_until_the_retry_delay()
    {
        var delay = TimeSpan.FromSeconds(2);
        await using var fixture = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                var serviceBus = Assert.IsType<AzureServiceBusTransportOptions>(options);
                serviceBus.RetryDelay = delay;
            },
            preHandler: null);

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

        var cancelledAt = DateTimeOffset.UtcNow;
        await cts.CancelAsync();
        await subscription.WaitAsync(TimeSpan.FromSeconds(10));

        await using var client = new ServiceBusClient(_container.AmqpConnectionString);
        await using var receiver = client.CreateReceiver(
            AzureServiceBusNames.Topic("messaging.conformance-ping"),
            fixture.Options.SubscriptionName);
        var early = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(400));
        Assert.Null(early);

        var later = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(later);
        Assert.True(DateTimeOffset.UtcNow - cancelledAt >= delay - TimeSpan.FromMilliseconds(200));
        await receiver.CompleteMessageAsync(later);
    }

    [Fact]
    public async Task A_failed_attempt_is_redelivered_with_the_subscription_on_the_span()
    {
        using var listener = StartListener();
        await using var fixture = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options => options.MaxDeliveryAttempts = 3,
            preHandler: null);

        var attempts = 0;
        bool? redelivered = null;
        string? subscriptionName = null;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await fixture.Transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            var attempt = Interlocked.Increment(ref attempts);
            if (attempt == 1)
                throw new InvalidOperationException("again");

            redelivered = Activity.Current?.GetTagItem("messaging.servicebus.redelivered") is true;
            subscriptionName = Activity.Current?.GetTagItem("messaging.servicebus.subscription") as string;
            done.TrySetResult();
            return Task.CompletedTask;
        });

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "redeliver"),
            MessageContext.New());
        await done.Task.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.True(redelivered);
        Assert.Equal(fixture.Options.SubscriptionName, subscriptionName);
    }

    [Fact]
    public async Task Exhausted_message_lands_on_the_dead_letter_subqueue_and_is_logged()
    {
        var logger = new WarningLogger();
        await using var fixture = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                options.MaxDeliveryAttempts = 2;
                options.DeadLetterFailedMessages = true;
            },
            preHandler: null,
            logger);

        var attempts = 0;
        await fixture.Transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("park-me");
        });

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "dead-letter-body"),
            new MessageContext { MessageId = "dlq-1" });

        await WaitUntilAsync(() => fixture.DeadLetters.Count >= 1);
        await Task.Delay(200);
        Assert.Equal(2, Volatile.Read(ref attempts));

        var parked = Assert.Single(fixture.DeadLetters);
        Assert.Equal(2, parked.Attempts);
        Assert.Equal("dlq-1", parked.Context.MessageId);

        var warning = Assert.Single(logger.Warnings);
        Assert.Contains("dlq-1", warning, StringComparison.Ordinal);
        Assert.Contains("Dead-lettered", warning, StringComparison.Ordinal);

        await using var client = new ServiceBusClient(_container.AmqpConnectionString);
        await using var receiver = client.CreateReceiver(
            AzureServiceBusNames.Topic("messaging.conformance-ping"),
            fixture.Options.SubscriptionName,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var dead = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(dead);
        Assert.Contains("dead-letter-body", Encoding.UTF8.GetString(dead.Body.ToArray()), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Dead_letter_off_schedules_the_message_again()
    {
        await using var fixture = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                options.MaxDeliveryAttempts = 2;
                options.DeadLetterFailedMessages = false;
            },
            preHandler: null);

        var calls = 0;
        using var cts = new CancellationTokenSource();
        var subscription = fixture.Transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            Interlocked.Increment(ref calls);
            throw new InvalidOperationException("keep");
        }, cts.Token);

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "kept"),
            MessageContext.New());

        await WaitUntilAsync(() => Volatile.Read(ref calls) >= 3);
        Assert.Empty(fixture.DeadLetters);

        await cts.CancelAsync();
        await subscription.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task The_handler_receives_the_subscription_cancellation_token()
    {
        await using var fixture = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            configure: null,
            preHandler: null);
        using var cts = new CancellationTokenSource();
        CancellationToken seen = default;
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = fixture.Transport.SubscribeAsync<ConformancePing>((_, _, ct) =>
        {
            seen = ct;
            delivered.TrySetResult();
            return Task.CompletedTask;
        }, cts.Token);

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "token"),
            MessageContext.New());
        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(seen.CanBeCanceled);
        await cts.CancelAsync();
        Assert.True(seen.IsCancellationRequested);
        await subscription.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Prefetch_limits_how_many_messages_are_handled_at_once()
    {
        await using var fixture = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                options.Prefetch = 2;
                options.Concurrency = 2;
            },
            preHandler: null);

        var entered = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await fixture.Transport.SubscribeAsync<ConformancePing>(async (_, _, ct) =>
        {
            Interlocked.Increment(ref entered);
            await release.Task.WaitAsync(ct);
        });

        for (var i = 0; i < 5; i++)
        {
            await fixture.Transport.PublishAsync(
                new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "prefetch-" + i),
                MessageContext.New());
        }

        await WaitUntilAsync(() => Volatile.Read(ref entered) >= 2);
        await Task.Delay(400);
        Assert.Equal(2, Volatile.Read(ref entered));

        release.TrySetResult();
        await WaitUntilAsync(() => Volatile.Read(ref entered) >= 5);
    }

    [Fact]
    public async Task Topology_setup_creates_the_topic_and_subscription()
    {
        var subscriptionName = "topology-" + Guid.NewGuid().ToString("N")[..8];
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAzureServiceBusTransport(options =>
        {
            options.ConnectionString = _container.AmqpConnectionString;
            options.AdministrationConnectionString = _container.AdminConnectionString;
            options.SubscriptionName = subscriptionName;
            options.SetupTopology = true;
        });
        services.AddSingleton(new MessageSubscription(typeof(TopologyPing)));

        await using var provider = services.BuildServiceProvider();
        var check = provider.GetServices<IHostedService>().OfType<MessageTransportStartupCheck>().Single();
        await check.StartAsync(CancellationToken.None);

        var setup = provider.GetRequiredService<AzureServiceBusTopologySetup>();
        var declared = MessageTypeNames.Require(typeof(TopologyPing));
        var topic = AzureServiceBusNames.Topic(declared);
        Assert.Contains(topic, setup.Topics);

        var admin = new ServiceBusAdministrationClient(_container.AdminConnectionString);
        Assert.True(await admin.TopicExistsAsync(topic));
        Assert.True(await admin.SubscriptionExistsAsync(topic, subscriptionName));

        await using var client = new ServiceBusClient(_container.AmqpConnectionString);
        await using var sender = client.CreateSender(topic);
        await sender.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes("topology-binding")));
        await using var receiver = client.CreateReceiver(topic, subscriptionName);
        var got = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(got);
        Assert.Equal("topology-binding", Encoding.UTF8.GetString(got.Body.ToArray()));
    }

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
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met before the timeout.");
            await Task.Delay(20);
        }
    }

    [EventTypeName("messaging.servicebus.publish-only")]
    private sealed record PublishOnlyPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName("messaging.servicebus.topology-ping")]
    private sealed record TopologyPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    private sealed class WarningLogger : ILogger<AzureServiceBusMessageTransport>
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
