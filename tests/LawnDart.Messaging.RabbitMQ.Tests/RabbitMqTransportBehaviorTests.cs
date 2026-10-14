using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using LawnDart.EventStore;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.Telemetry;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.RabbitMQ.Tests;

[Collection(RabbitMqCollection.Name)]
[Trait("Category", "Integration")]
public sealed class RabbitMqTransportBehaviorTests
{
    private readonly RabbitMqContainerFixture _container;

    public RabbitMqTransportBehaviorTests(RabbitMqContainerFixture container) => _container = container;

    [Fact]
    public async Task Prefetch_limits_how_many_messages_are_handled_at_once()
    {
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            options.Prefetch = 2;
            options.Concurrency = 2;
        }, preHandler: null);

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
        await Task.Delay(300);
        Assert.Equal(2, Volatile.Read(ref entered));

        release.TrySetResult();
        await WaitUntilAsync(() => Volatile.Read(ref entered) >= 5);
    }

    [Fact]
    public async Task A_failed_attempt_is_redelivered_with_the_queue_name_on_the_span()
    {
        using var listener = StartListener();
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            options.MaxDeliveryAttempts = 3;
        }, preHandler: null);

        var attempts = 0;
        bool? redelivered = null;
        string? queue = null;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await fixture.Transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            var attempt = Interlocked.Increment(ref attempts);
            if (attempt == 1)
                throw new InvalidOperationException("again");

            redelivered = Activity.Current?.GetTagItem("messaging.rabbitmq.redelivered") is true;
            queue = Activity.Current?.GetTagItem("messaging.rabbitmq.queue") as string;
            done.TrySetResult();
            return Task.CompletedTask;
        });

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "redeliver"),
            MessageContext.New());
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(redelivered);
        Assert.Equal(
            RabbitMqNames.Queue("messaging.conformance-ping", fixture.Options.SubscriptionName),
            queue);
    }

    [Fact]
    public async Task Exhausted_message_lands_on_the_dead_letter_queue_and_is_logged()
    {
        var logger = new WarningLogger();
        await using var fixture = RabbitMqTransportFixture.Create(
            _container.AmqpUri,
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
            new MessageContext { MessageId = "dlx-1" });

        await WaitUntilAsync(() => fixture.DeadLetters.Count >= 1);
        await Task.Delay(200);
        Assert.Equal(2, Volatile.Read(ref attempts));

        var parked = Assert.Single(fixture.DeadLetters);
        Assert.Equal(2, parked.Attempts);
        Assert.Equal("dlx-1", parked.Context.MessageId);

        var warning = Assert.Single(logger.Warnings);
        Assert.Contains("dlx-1", warning, StringComparison.Ordinal);
        Assert.Contains("Dead-lettered", warning, StringComparison.Ordinal);

        var queue = RabbitMqNames.DeadLetterQueue("messaging.conformance-ping", fixture.Options.SubscriptionName);
        var body = await ReadQueueAsync(queue);
        Assert.Contains("dead-letter-body", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Dead_letter_off_nacks_the_message_back_onto_the_queue()
    {
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            options.MaxDeliveryAttempts = 2;
            options.DeadLetterFailedMessages = false;
        }, preHandler: null);

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

        var deadLetterQueue = RabbitMqNames.DeadLetterQueue(
            "messaging.conformance-ping",
            fixture.Options.SubscriptionName);
        Assert.Equal(0u, await CountAsync(deadLetterQueue));

        await cts.CancelAsync();
        await subscription.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task The_handler_receives_the_subscription_cancellation_token()
    {
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, configure: null, preHandler: null);
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
    public async Task Topology_setup_declares_the_exchange_queue_binding_and_dead_letter_pair()
    {
        var subscriptionName = "topology-" + Guid.NewGuid().ToString("N")[..8];
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMqTransport(options =>
        {
            options.Uri = _container.AmqpUri;
            options.SubscriptionName = subscriptionName;
            options.SetupTopology = true;
        });
        services.AddSingleton(new MessageSubscription(typeof(TopologyPing)));

        await using var provider = services.BuildServiceProvider();
        var check = provider.GetServices<IHostedService>().OfType<MessageTransportStartupCheck>().Single();
        await check.StartAsync(CancellationToken.None);

        var setup = provider.GetRequiredService<RabbitMqTopologySetup>();
        var declared = MessageTypeNames.Require(typeof(TopologyPing));
        var queue = RabbitMqNames.Queue(declared, subscriptionName);
        Assert.Contains(queue, setup.Queues);

        await using var connection = await OpenAsync();
        await using var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(true, true, null, 1));

        await channel.ExchangeDeclareAsync(
            RabbitMqNames.Exchange(declared),
            ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            passive: true);
        await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, passive: true);
        await channel.ExchangeDeclareAsync(
            RabbitMqNames.DeadLetterExchange(declared),
            ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            passive: true);
        await channel.QueueDeclareAsync(
            RabbitMqNames.DeadLetterQueue(declared, subscriptionName),
            durable: true,
            exclusive: false,
            autoDelete: false,
            passive: true);

        var payload = Encoding.UTF8.GetBytes("topology-binding");
        await channel.BasicPublishAsync(
            RabbitMqNames.Exchange(declared),
            RabbitMqNames.RoutingKey(declared),
            mandatory: true,
            new BasicProperties { DeliveryMode = DeliveryModes.Persistent },
            payload);

        var got = await channel.BasicGetAsync(queue, autoAck: true);
        Assert.NotNull(got);
        Assert.Equal("topology-binding", Encoding.UTF8.GetString(got.Body.ToArray()));
    }

    private async Task<string> ReadQueueAsync(string queue)
    {
        await using var connection = await OpenAsync();
        await using var channel = await connection.CreateChannelAsync();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            var message = await channel.BasicGetAsync(queue, autoAck: true);
            if (message is not null)
                return Encoding.UTF8.GetString(message.Body.ToArray());
            await Task.Delay(50);
        }

        throw new TimeoutException($"Queue '{queue}' had no message.");
    }

    private async Task<uint> CountAsync(string queue)
    {
        await using var connection = await OpenAsync();
        await using var channel = await connection.CreateChannelAsync();
        return await channel.MessageCountAsync(queue);
    }

    private async Task<IConnection> OpenAsync()
    {
        var factory = new ConnectionFactory { Uri = new Uri(_container.AmqpUri) };
        return await factory.CreateConnectionAsync();
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
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met before the timeout.");
            await Task.Delay(20);
        }
    }

    [EventTypeName("messaging.rabbitmq.topology-ping")]
    private sealed record TopologyPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

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
