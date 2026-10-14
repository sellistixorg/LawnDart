using Azure.Messaging.ServiceBus.Administration;
using LawnDart.EventStore;
using LawnDart.Messaging.Testing;

namespace LawnDart.Messaging.AzureServiceBus.Tests;

[Collection(AzureServiceBusCollection.Name)]
[Trait("Category", "Integration")]
public sealed class AzureServiceBusTopicMappingTests
{
    private const string PlacedName = "messaging.topic-map.placed";
    private const string ShippedName = "messaging.topic-map.shipped";

    private readonly AzureServiceBusContainerFixture _container;

    public AzureServiceBusTopicMappingTests(AzureServiceBusContainerFixture container) => _container = container;

    [Fact]
    public async Task A_grouped_topic_delivers_each_type_only_to_its_subscriber_and_retries_only_the_failure()
    {
        var topic = "topic-map-" + Guid.NewGuid().ToString("N")[..8];
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var mapper = Group(topic);
        var placedSubscription = "placed-" + suffix;
        var shippedSubscription = "shipped-" + suffix;

        await using var placed = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                options.SubscriptionName = placedSubscription;
                options.MaxDeliveryAttempts = 3;
                options.TopicMapper = mapper;
            },
            preHandler: null);
        await using var shipped = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                options.SubscriptionName = shippedSubscription;
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

        await placed.Transport.PublishAsync(
            new PlacedPing(Guid.NewGuid(), DateTime.UtcNow, "placed"),
            MessageContext.New());
        await shipped.Transport.PublishAsync(
            new ShippedPing(Guid.NewGuid(), DateTime.UtcNow, "shipped"),
            MessageContext.New());

        await placedDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await shippedDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await Task.Delay(800);

        Assert.Equal(["placed", "placed"], Snapshot(placedTexts));
        Assert.Equal(["shipped"], Snapshot(shippedTexts));

        var admin = new ServiceBusAdministrationClient(_container.AdminConnectionString);
        var placedRule = await admin.GetRuleAsync(topic, placedSubscription, AzureServiceBusHeaders.RouteRuleName);
        var shippedRule = await admin.GetRuleAsync(topic, shippedSubscription, AzureServiceBusHeaders.RouteRuleName);
        var placedSql = Assert.IsType<SqlRuleFilter>(placedRule.Value.Filter).SqlExpression;
        var shippedSql = Assert.IsType<SqlRuleFilter>(shippedRule.Value.Filter).SqlExpression;
        Assert.Contains("lawndart_target IS NULL", placedSql, StringComparison.Ordinal);
        Assert.Contains(PlacedName, placedSql, StringComparison.Ordinal);
        Assert.DoesNotContain(ShippedName, placedSql, StringComparison.Ordinal);
        Assert.Contains("lawndart_target IS NULL", shippedSql, StringComparison.Ordinal);
        Assert.Contains(ShippedName, shippedSql, StringComparison.Ordinal);
        Assert.DoesNotContain(PlacedName, shippedSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task One_subscription_on_a_grouped_topic_dispatches_each_type_to_its_handler()
    {
        var topic = "topic-map-one-" + Guid.NewGuid().ToString("N")[..8];
        await using var fixture = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                options.MaxDeliveryAttempts = 3;
                options.TopicMapper = Group(topic);
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

        await fixture.Transport.PublishAsync(
            new PlacedPing(Guid.NewGuid(), DateTime.UtcNow, "placed"),
            MessageContext.New());
        await fixture.Transport.PublishAsync(
            new ShippedPing(Guid.NewGuid(), DateTime.UtcNow, "shipped"),
            MessageContext.New());

        await placedDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await shippedDone.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await Task.Delay(800);

        Assert.Equal(["placed", "placed"], Snapshot(placedTexts));
        Assert.Equal(["shipped"], Snapshot(shippedTexts));

        var admin = new ServiceBusAdministrationClient(_container.AdminConnectionString);
        var rule = await admin.GetRuleAsync(topic, fixture.Options.SubscriptionName, AzureServiceBusHeaders.RouteRuleName);
        var sql = Assert.IsType<SqlRuleFilter>(rule.Value.Filter).SqlExpression;
        Assert.Contains("lawndart_target IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains(PlacedName, sql, StringComparison.Ordinal);
        Assert.Contains(ShippedName, sql, StringComparison.Ordinal);
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
