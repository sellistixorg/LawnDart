using RabbitMQ.Client;
using LawnDart.EventStore;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.RabbitMQ.Tests;

[Collection(RabbitMqCollection.Name)]
[Trait("Category", "Integration")]
public sealed class RabbitMqTopicMappingTests
{
    private const string PlacedName = "messaging.topic-map.placed";
    private const string ShippedName = "messaging.topic-map.shipped";

    private readonly RabbitMqContainerFixture _container;

    public RabbitMqTopicMappingTests(RabbitMqContainerFixture container) => _container = container;

    [Theory]
    [InlineData(RabbitMqQueueLayout.Subscription)]
    [InlineData(RabbitMqQueueLayout.EventType)]
    public async Task A_grouped_topic_delivers_each_type_only_to_its_subscriber_and_retries_only_the_failure(
        RabbitMqQueueLayout layout)
    {
        var topic = "topic-map-" + Guid.NewGuid().ToString("N")[..8];
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var mapper = Group(topic);

        await using var placed = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.QueueLayout = layout;
            options.SubscriptionName = "placed-" + suffix;
            options.MaxDeliveryAttempts = 3;
            options.TopicMapper = mapper;
        }, preHandler: null);
        await using var shipped = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.QueueLayout = layout;
            options.SubscriptionName = "shipped-" + suffix;
            options.TopicMapper = mapper;
        }, preHandler: null);

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

        await placed.Transport.PublishAsync(
            new PlacedPing(Guid.NewGuid(), DateTime.UtcNow, "placed"),
            MessageContext.New());
        await shipped.Transport.PublishAsync(
            new ShippedPing(Guid.NewGuid(), DateTime.UtcNow, "shipped"),
            MessageContext.New());

        await placedDone.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await shippedDone.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await Task.Delay(600);

        Assert.Equal(["placed", "placed"], Snapshot(placedTexts));
        Assert.Equal(["shipped"], Snapshot(shippedTexts));

        await using var connection = await OpenAsync();
        await using var channel = await connection.CreateChannelAsync();
        await channel.ExchangeDeclareAsync(topic, "direct", durable: true, autoDelete: false, passive: true);
    }

    [Theory]
    [InlineData(RabbitMqQueueLayout.Subscription)]
    [InlineData(RabbitMqQueueLayout.EventType)]
    public async Task One_subscription_on_a_grouped_topic_dispatches_each_type_to_its_handler(
        RabbitMqQueueLayout layout)
    {
        var topic = "topic-map-one-" + Guid.NewGuid().ToString("N")[..8];
        await using var fixture = RabbitMqTransportFixture.Create(_container.AmqpUri, options =>
        {
            var rabbit = Assert.IsType<RabbitMqTransportOptions>(options);
            rabbit.QueueLayout = layout;
            options.MaxDeliveryAttempts = 3;
            options.TopicMapper = Group(topic);
        }, preHandler: null);

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

        await fixture.Transport.PublishAsync(
            new PlacedPing(Guid.NewGuid(), DateTime.UtcNow, "placed"),
            MessageContext.New());
        await fixture.Transport.PublishAsync(
            new ShippedPing(Guid.NewGuid(), DateTime.UtcNow, "shipped"),
            MessageContext.New());

        await placedDone.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await shippedDone.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await Task.Delay(600);

        Assert.Equal(["placed", "placed"], Snapshot(placedTexts));
        Assert.Equal(["shipped"], Snapshot(shippedTexts));
    }

    private async Task<IConnection> OpenAsync()
    {
        var factory = new ConnectionFactory { Uri = new Uri(_container.AmqpUri) };
        return await factory.CreateConnectionAsync();
    }

    private static GroupTopicMapper Group(string topic) => new(new Dictionary<string, string>
    {
        [PlacedName] = topic,
        [ShippedName] = topic,
    });

    private static List<string> Snapshot(List<string> texts)
    {
        lock (texts)
            return [.. texts];
    }

    [EventTypeName(PlacedName)]
    private sealed record PlacedPing(Guid Id, DateTime Timestamp, string Text) : IEvent;

    [EventTypeName(ShippedName)]
    private sealed record ShippedPing(Guid Id, DateTime Timestamp, string Text) : IEvent;
}
