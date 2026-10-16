using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using LawnDart.EventStore;
using LawnDart.Messaging.Hosting;

namespace LawnDart.Messaging.RabbitMQ.Tests;

[Collection(RabbitMqCollection.Name)]
[Trait("Category", "Integration")]
public sealed class RabbitMqQueueLayoutTests
{
    private const string PlacedName = "messaging.rabbitmq.layout-placed";
    private const string ShippedName = "messaging.rabbitmq.layout-shipped";

    private readonly RabbitMqContainerFixture _container;

    public RabbitMqQueueLayoutTests(RabbitMqContainerFixture container) => _container = container;

    [Theory]
    [InlineData(RabbitMqQueueLayout.Subscription)]
    [InlineData(RabbitMqQueueLayout.EventType)]
    public async Task Topology_declares_subscription_names_and_routes_by_event_type(RabbitMqQueueLayout layout)
    {
        var subscription = "layout-" + Guid.NewGuid().ToString("N")[..8];
        var topic = "layout-topic-" + Guid.NewGuid().ToString("N")[..8];
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMqTransport(options =>
        {
            options.Uri = _container.AmqpUri;
            options.SubscriptionName = subscription;
            options.QueueLayout = layout;
            options.SetupTopology = true;
            options.RetryDelay = TimeSpan.FromMilliseconds(200);
            options.TopicMapper = new GroupTopicMapper(new Dictionary<string, string>
            {
                [PlacedName] = topic,
                [ShippedName] = topic,
            });
        });
        services.AddSingleton(new MessageSubscription(typeof(LayoutPlaced)));
        services.AddSingleton(new MessageSubscription(typeof(LayoutShipped)));

        await using var provider = services.BuildServiceProvider();
        var check = provider.GetServices<IHostedService>().OfType<MessageTransportStartupCheck>().Single();
        await check.StartAsync(CancellationToken.None);

        var setup = provider.GetRequiredService<RabbitMqTopologySetup>();
        var placedQueue = RabbitMqNames.Queue(subscription, layout, PlacedName);
        var shippedQueue = RabbitMqNames.Queue(subscription, layout, ShippedName);
        Assert.Contains(placedQueue, setup.Queues);
        Assert.Contains(shippedQueue, setup.Queues);
        if (layout == RabbitMqQueueLayout.Subscription)
            Assert.Single(setup.Queues);
        else
            Assert.Equal(2, setup.Queues.Count);

        var queues = await NamesAsync("list_queues", "name");
        var exchanges = await NamesAsync("list_exchanges", "name");
        var bindings = await _container.RabbitMqCtlAsync("-q", "list_bindings", "source_name", "destination_name", "routing_key");

        var ownedQueues = queues
            .Where(name => name == subscription || name.StartsWith(subscription + ".", StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);
        var expectedQueues = new HashSet<string>(StringComparer.Ordinal)
        {
            placedQueue,
            shippedQueue,
            RabbitMqNames.RetryQueue(subscription),
            RabbitMqNames.DeadLetterQueue(subscription),
        };
        Assert.Equal(expectedQueues, ownedQueues);

        Assert.Contains(topic, exchanges);
        Assert.Contains(RabbitMqNames.DelayExchange(subscription), exchanges);
        Assert.Contains(RabbitMqNames.DeadLetterExchange(subscription), exchanges);
        Assert.Contains(RabbitMqNames.RetryExchange(subscription), exchanges);
        Assert.DoesNotContain(PlacedName + ".dlx", exchanges);
        Assert.DoesNotContain(ShippedName + ".dlx", exchanges);

        AssertBinding(bindings, topic, placedQueue, PlacedName);
        AssertBinding(bindings, topic, shippedQueue, ShippedName);
        AssertBinding(bindings, RabbitMqNames.DelayExchange(subscription), placedQueue, PlacedName);
        AssertBinding(bindings, RabbitMqNames.DelayExchange(subscription), shippedQueue, ShippedName);
        AssertBinding(bindings, RabbitMqNames.RetryExchange(subscription), RabbitMqNames.RetryQueue(subscription), PlacedName);
        AssertBinding(bindings, RabbitMqNames.RetryExchange(subscription), RabbitMqNames.RetryQueue(subscription), ShippedName);
        AssertBinding(bindings, RabbitMqNames.DeadLetterExchange(subscription), RabbitMqNames.DeadLetterQueue(subscription), PlacedName);
        AssertBinding(bindings, RabbitMqNames.DeadLetterExchange(subscription), RabbitMqNames.DeadLetterQueue(subscription), ShippedName);

        await using var connection = await OpenAsync();
        await using var channel = await connection.CreateChannelAsync();
        await PublishAsync(channel, topic, PlacedName, "placed-body");
        await PublishAsync(channel, topic, ShippedName, "shipped-body");
        Assert.Equal("placed-body", await ReadQueueAsync(channel, placedQueue));
        Assert.Equal("shipped-body", await ReadQueueAsync(channel, shippedQueue));

        await PublishAsync(channel, RabbitMqNames.DelayExchange(subscription), PlacedName, "delay-body");
        Assert.Equal("delay-body", await ReadQueueAsync(channel, placedQueue));

        await PublishAsync(channel, RabbitMqNames.DeadLetterExchange(subscription), ShippedName, "dlq-body");
        Assert.Equal("dlq-body", await ReadQueueAsync(channel, RabbitMqNames.DeadLetterQueue(subscription)));

        await PublishAsync(channel, RabbitMqNames.RetryExchange(subscription), PlacedName, "retry-body");
        Assert.Equal("retry-body", await ReadQueueAsync(channel, placedQueue));
    }

    [Theory]
    [InlineData(RabbitMqQueueLayout.Subscription)]
    [InlineData(RabbitMqQueueLayout.EventType)]
    public async Task Retry_then_dead_letter_lands_in_the_subscription_dlq(RabbitMqQueueLayout layout)
    {
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.QueueLayout = layout;
            options.MaxDeliveryAttempts = 2;
            options.DeadLetterFailedMessages = true;
        }, preHandler: null);

        var attempts = 0;
        await fixture.Transport.SubscribeAsync<LayoutPlaced>((_, _, _) =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("park");
        });
        await fixture.Transport.SubscribeAsync<LayoutShipped>((_, _, _) =>
            throw new InvalidOperationException("park-shipped"));

        await fixture.Transport.PublishAsync(
            new LayoutPlaced(Guid.NewGuid(), DateTime.UtcNow, "dead-letter-body"),
            new MessageContext { MessageId = "layout-dlq" });

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (fixture.DeadLetters.Count < 1)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The message was not dead-lettered.");
            await Task.Delay(20);
        }

        Assert.Equal(2, Volatile.Read(ref attempts));
        var parked = Assert.Single(fixture.DeadLetters);
        Assert.Equal(PlacedName, parked.MessageType);
        Assert.Equal(2, parked.Attempts);

        var subscription = fixture.Options.SubscriptionName;
        var dlq = RabbitMqNames.DeadLetterQueue(subscription);
        await using var connection = await OpenAsync();
        await using var channel = await connection.CreateChannelAsync();
        Assert.Contains("dead-letter-body", await ReadQueueAsync(channel, dlq), StringComparison.Ordinal);
        Assert.Equal(0u, await channel.MessageCountAsync(RabbitMqNames.Queue(subscription, layout, PlacedName)));
        Assert.Equal(0u, await channel.MessageCountAsync(RabbitMqNames.RetryQueue(subscription)));

        var perEventDlq = subscription + "." + PlacedName + ".dlq";
        var missing = await Record.ExceptionAsync(() => channel.QueueDeclareAsync(
            perEventDlq,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            passive: true));
        Assert.NotNull(missing);
    }

    [Theory]
    [InlineData(RabbitMqQueueLayout.Subscription)]
    [InlineData(RabbitMqQueueLayout.EventType)]
    public async Task Two_subscriptions_on_one_event_type_each_receive_a_copy(RabbitMqQueueLayout layout)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        await using var first = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.QueueLayout = layout;
            options.SubscriptionName = "copy-a-" + suffix;
        }, preHandler: null);
        await using var second = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.QueueLayout = layout;
            options.SubscriptionName = "copy-b-" + suffix;
        }, preHandler: null);

        var firstTexts = new List<string>();
        var secondTexts = new List<string>();
        var firstDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await first.Transport.SubscribeAsync<LayoutPlaced>((message, _, _) =>
        {
            lock (firstTexts)
                firstTexts.Add(message.Text);
            firstDone.TrySetResult();
            return Task.CompletedTask;
        });
        await second.Transport.SubscribeAsync<LayoutPlaced>((message, _, _) =>
        {
            lock (secondTexts)
                secondTexts.Add(message.Text);
            secondDone.TrySetResult();
            return Task.CompletedTask;
        });

        await first.Transport.PublishAsync(
            new LayoutPlaced(Guid.NewGuid(), DateTime.UtcNow, "shared"),
            MessageContext.New());

        await firstDone.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await secondDone.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(["shared"], Snapshot(firstTexts));
        Assert.Equal(["shared"], Snapshot(secondTexts));
    }

    private async Task<HashSet<string>> NamesAsync(string command, string column)
    {
        var output = await _container.RabbitMqCtlAsync("-q", command, column);
        return output
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(line => line != column)
            .ToHashSet(StringComparer.Ordinal);
    }

    private static void AssertBinding(string table, string source, string destination, string routingKey)
    {
        foreach (var line in table.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var parts = line.Split('\t', StringSplitOptions.TrimEntries);
            if (parts.Length >= 3 && parts[0] == source && parts[1] == destination && parts[2] == routingKey)
                return;
        }

        Assert.Fail($"Missing binding {source} -> {destination} with routing key {routingKey}.{Environment.NewLine}{table}");
    }

    private static async Task PublishAsync(IChannel channel, string exchange, string routingKey, string text)
    {
        await channel.BasicPublishAsync(
            exchange,
            routingKey,
            mandatory: true,
            new BasicProperties { DeliveryMode = DeliveryModes.Persistent },
            Encoding.UTF8.GetBytes(text));
    }

    private static async Task<string> ReadQueueAsync(IChannel channel, string queue)
    {
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

    private async Task<IConnection> OpenAsync()
    {
        var factory = new ConnectionFactory { Uri = new Uri(_container.AmqpUri) };
        return await factory.CreateConnectionAsync();
    }

    private static List<string> Snapshot(List<string> texts)
    {
        lock (texts)
            return [.. texts];
    }

    [EventTypeName(PlacedName)]
    private sealed record LayoutPlaced(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName(ShippedName)]
    private sealed record LayoutShipped(Guid Id, DateTime Timestamp, string Text) : IEvent;
}
