using System.Text.Json;
using Azure.Messaging.ServiceBus;
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

        var sql = await RouteSqlAsync(topic, fixture.Options.SubscriptionName);
        Assert.Contains("lawndart_target IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains(PlacedName, sql, StringComparison.Ordinal);
        Assert.Contains(ShippedName, sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_restart_keeps_messages_published_between_subscribes_on_a_grouped_topic()
    {
        var topic = "topic-restart-" + Guid.NewGuid().ToString("N")[..8];
        var subscription = "sub-" + Guid.NewGuid().ToString("N")[..8];
        var mapper = Group(topic);
        var planned = new MessageSubscription[]
        {
            new(typeof(PlacedPing)),
            new(typeof(ShippedPing)),
        };

        await using (var first = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                options.SubscriptionName = subscription;
                options.TopicMapper = mapper;
            },
            preHandler: null,
            subscriptions: planned))
        {
            await first.Transport.SubscribeAsync<PlacedPing>((_, _, _) => Task.CompletedTask);
            await first.Transport.SubscribeAsync<ShippedPing>((_, _, _) => Task.CompletedTask);
        }

        await using var restarted = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                options.SubscriptionName = subscription;
                options.TopicMapper = mapper;
            },
            preHandler: null,
            subscriptions: planned);

        var placed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shipped = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await restarted.Transport.SubscribeAsync<PlacedPing>((message, _, _) =>
        {
            placed.TrySetResult(message.Text);
            return Task.CompletedTask;
        });

        var duringRestart = await RouteSqlAsync(topic, subscription);
        Assert.Contains(PlacedName, duringRestart, StringComparison.Ordinal);
        Assert.Contains(ShippedName, duringRestart, StringComparison.Ordinal);

        var placedId = "placed-" + Guid.NewGuid().ToString("N")[..8];
        var shippedId = "shipped-" + Guid.NewGuid().ToString("N")[..8];
        await restarted.Transport.PublishAsync(
            new PlacedPing(Guid.NewGuid(), DateTime.UtcNow, placedId),
            MessageContext.New());
        await restarted.Transport.PublishAsync(
            new ShippedPing(Guid.NewGuid(), DateTime.UtcNow, shippedId),
            MessageContext.New());

        await restarted.Transport.SubscribeAsync<ShippedPing>((message, _, _) =>
        {
            shipped.TrySetResult(message.Text);
            return Task.CompletedTask;
        });

        Assert.Equal(placedId, await placed.Task.WaitAsync(TimeSpan.FromSeconds(20)));
        Assert.Equal(shippedId, await shipped.Task.WaitAsync(TimeSpan.FromSeconds(20)));
    }

    [Fact]
    public async Task Upgrading_a_target_only_rule_does_not_drop_messages()
    {
        var topic = "topic-upgrade-" + Guid.NewGuid().ToString("N")[..8];
        var subscription = "up-" + Guid.NewGuid().ToString("N")[..8];
        var mapper = new GroupTopicMapper(new Dictionary<string, string>
        {
            [PlacedName] = topic,
        });
        var admin = new ServiceBusAdministrationClient(_container.AdminConnectionString);
        await admin.CreateTopicAsync(topic);
        await admin.CreateSubscriptionAsync(
            new CreateSubscriptionOptions(topic, subscription) { MaxDeliveryCount = 10 },
            new CreateRuleOptions(
                AzureServiceBusHeaders.RouteRuleName,
                new SqlRuleFilter(AzureServiceBusHeaders.RetryFilter(subscription))));

        await using var client = new ServiceBusClient(_container.AmqpConnectionString);
        await using var sender = client.CreateSender(topic);
        var sent = new List<string>();
        var received = new HashSet<string>(StringComparer.Ordinal);
        var gate = new object();

        async Task SendAsync(string id)
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(
                new PlacedPing(Guid.NewGuid(), DateTime.UtcNow, id),
                MessagingJson.Options);
            var message = new ServiceBusMessage(body)
            {
                ContentType = "application/json",
                Subject = PlacedName,
                MessageId = id,
            };
            message.ApplicationProperties[AzureServiceBusHeaders.TargetSubscription] = AzureServiceBusHeaders.TargetAll;
            message.ApplicationProperties[AzureServiceBusHeaders.EventType] = PlacedName;
            message.ApplicationProperties[MessageHeaders.MessageType] = PlacedName;
            await sender.SendMessageAsync(message);
            lock (gate)
                sent.Add(id);
        }

        for (var i = 0; i < 5; i++)
            await SendAsync("before-" + i);

        var publish = 0;
        var pumping = true;
        var pump = Task.Run(async () =>
        {
            while (Volatile.Read(ref pumping) && publish < 30)
            {
                var id = "during-" + Interlocked.Increment(ref publish);
                await SendAsync(id);
                await Task.Delay(15);
            }
        });

        await using var transport = AzureServiceBusTransportFixture.Create(
            _container.AmqpConnectionString,
            _container.AdminConnectionString,
            options =>
            {
                options.SubscriptionName = subscription;
                options.TopicMapper = mapper;
            },
            preHandler: null,
            subscriptions: [new MessageSubscription(typeof(PlacedPing))]);

        await transport.Transport.SubscribeAsync<PlacedPing>((message, _, _) =>
        {
            lock (gate)
                received.Add(message.Text);
            return Task.CompletedTask;
        });

        Volatile.Write(ref pumping, false);
        await pump;
        await SendAsync("after");

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (DateTime.UtcNow < deadline)
        {
            lock (gate)
            {
                if (sent.All(received.Contains))
                    break;
            }

            await Task.Delay(50);
        }

        List<string> missing;
        lock (gate)
            missing = sent.Where(id => !received.Contains(id)).ToList();
        Assert.Empty(missing);

        var rules = new List<RuleProperties>();
        await foreach (var rule in admin.GetRulesAsync(topic, subscription))
            rules.Add(rule);

        var routes = rules.Where(rule => rule.Name.StartsWith(AzureServiceBusHeaders.RouteRuleName, StringComparison.Ordinal)).ToArray();
        var route = Assert.Single(routes);
        var sql = Assert.IsType<SqlRuleFilter>(route.Filter).SqlExpression;
        Assert.Equal(AzureServiceBusHeaders.RouteFilter(subscription, [PlacedName]), sql);
        Assert.DoesNotContain(rules, rule => rule.Name == "$Default" && rule.Filter is TrueRuleFilter);
    }

    private async Task<string> RouteSqlAsync(string topic, string subscription)
    {
        var admin = new ServiceBusAdministrationClient(_container.AdminConnectionString);
        var text = new System.Text.StringBuilder();
        await foreach (var rule in admin.GetRulesAsync(topic, subscription))
        {
            if (rule.Name.StartsWith(AzureServiceBusHeaders.RouteRuleName, StringComparison.Ordinal) &&
                rule.Filter is SqlRuleFilter sql)
            {
                text.AppendLine(sql.SqlExpression);
            }
        }

        return text.ToString();
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
