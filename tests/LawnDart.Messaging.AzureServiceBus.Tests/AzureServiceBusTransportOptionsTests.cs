using System.Diagnostics.Metrics;
using Azure.Core;
using LawnDart.EventStore;

namespace LawnDart.Messaging.AzureServiceBus.Tests;

public class AzureServiceBusTransportOptionsTests
{
    [Fact]
    public void Names_use_the_declared_type_and_subscription()
    {
        Assert.Equal("order.placed", AzureServiceBusNames.Topic("order.placed"));
        Assert.Equal("billing", AzureServiceBusNames.Subscription("billing"));
    }

    [Fact]
    public void A_topic_name_with_a_space_is_rejected()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => AzureServiceBusNames.Topic("order placed"));
        Assert.Contains("topic", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_subscription_name_over_50_characters_is_rejected()
    {
        var name = new string('a', 51);
        var ex = Assert.Throws<InvalidOperationException>(() => AzureServiceBusNames.Subscription(name));
        Assert.Contains("subscription", ex.Message, StringComparison.Ordinal);
        Assert.Contains("50", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_service_bus_connection_string_is_accepted()
    {
        var options = new AzureServiceBusTransportOptions
        {
            ConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE",
        };
        options.EnsureValid();
    }

    [Fact]
    public void An_http_endpoint_is_rejected()
    {
        var options = new AzureServiceBusTransportOptions { ConnectionString = "Endpoint=http://localhost/;SharedAccessKey=x" };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("ConnectionString", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Namespace_and_credential_are_accepted_together()
    {
        var options = new AzureServiceBusTransportOptions
        {
            FullyQualifiedNamespace = "my-namespace.servicebus.windows.net",
            Credential = new FakeCredential(),
        };
        options.EnsureValid();
    }

    [Fact]
    public void A_namespace_without_a_credential_is_rejected()
    {
        var options = new AzureServiceBusTransportOptions
        {
            FullyQualifiedNamespace = "my-namespace.servicebus.windows.net",
        };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("Credential", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_uri_is_rejected_as_a_namespace()
    {
        var options = new AzureServiceBusTransportOptions
        {
            FullyQualifiedNamespace = "sb://my-namespace.servicebus.windows.net",
            Credential = new FakeCredential(),
        };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("FullyQualifiedNamespace", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Connection_string_and_namespace_together_are_rejected()
    {
        var options = new AzureServiceBusTransportOptions
        {
            ConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE",
            FullyQualifiedNamespace = "my-namespace.servicebus.windows.net",
            Credential = new FakeCredential(),
        };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("not both", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void An_empty_connection_is_valid_until_a_client_is_required()
    {
        var options = new AzureServiceBusTransportOptions();
        options.EnsureValid();
        Assert.False(options.HasDataConnection);
        Assert.False(options.HasAdminConnection);
    }

    [Fact]
    public void Retry_delay_defaults_to_five_seconds_and_must_be_positive()
    {
        Assert.Equal(TimeSpan.FromSeconds(5), new AzureServiceBusTransportOptions().RetryDelay);
        Assert.Equal(TimeSpan.FromSeconds(30), new AzureServiceBusTransportOptions().OperationTimeout);
        Assert.Equal(2000, new AzureServiceBusTransportOptions().BrokerMaxDeliveryCount);

        var options = new AzureServiceBusTransportOptions { RetryDelay = TimeSpan.Zero };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("RetryDelay", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Broker_max_delivery_count_must_be_from_1_to_2000()
    {
        var low = new AzureServiceBusTransportOptions { BrokerMaxDeliveryCount = 0 };
        var lowError = Assert.Throws<InvalidOperationException>(low.EnsureValid);
        Assert.Contains("BrokerMaxDeliveryCount", lowError.Message, StringComparison.Ordinal);

        var high = new AzureServiceBusTransportOptions { BrokerMaxDeliveryCount = 2001 };
        var highError = Assert.Throws<InvalidOperationException>(high.EnsureValid);
        Assert.Contains("1 to 2000", highError.Message, StringComparison.Ordinal);

        var emulator = new AzureServiceBusTransportOptions { BrokerMaxDeliveryCount = 10 };
        emulator.EnsureValid();
    }

    [Fact]
    public void Retry_filter_accepts_a_publish_and_one_subscription()
    {
        Assert.Equal(
            "user.lawndart_target IS NULL OR user.lawndart_target = '*' OR user.lawndart_target = 'billing'",
            AzureServiceBusHeaders.RetryFilter("billing"));
    }

    [Fact]
    public void Route_filter_keeps_a_missing_target_and_matches_the_event_type()
    {
        Assert.Equal(AzureServiceBusNames.EventTypeProperty, AzureServiceBusHeaders.EventType);
        Assert.Equal(
            "(user.lawndart_target IS NULL OR user.lawndart_target = '*' OR user.lawndart_target = 'billing') AND (user.lawndart_type IS NULL OR user.lawndart_type = 'order-placed')",
            AzureServiceBusHeaders.RouteFilter("billing", ["order-placed"]));

        var grouped = AzureServiceBusHeaders.RouteFilter("bill'ing", ["zeta", "o'hare"]);
        Assert.Contains("bill''ing", grouped, StringComparison.Ordinal);
        Assert.Contains(
            "user.lawndart_type IS NULL OR user.lawndart_type = 'o''hare' OR user.lawndart_type = 'zeta'",
            grouped,
            StringComparison.Ordinal);
    }

    [Fact]
    public void A_target_only_rule_names_no_event_types_and_a_grouped_rule_names_each_type()
    {
        Assert.Empty(AzureServiceBusHeaders.EventTypesInSql(AzureServiceBusHeaders.RetryFilter("billing")));

        var sql = AzureServiceBusHeaders.RouteFilter("billing", ["zeta", "o'hare"]);
        Assert.Equal(["o'hare", "zeta"], AzureServiceBusHeaders.EventTypesInSql(sql));
    }

    [Fact]
    public void A_route_filter_over_1024_characters_is_rejected()
    {
        var types = Enumerable.Range(0, 30).Select(i => new string('n', 40) + i.ToString("00")).ToArray();
        var ex = Assert.Throws<InvalidOperationException>(() => AzureServiceBusHeaders.RouteFilter("billing", types));
        Assert.Contains("1024", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Attempt_property_stays_off_the_envelope_headers()
    {
        var properties = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            [MessageHeaders.UserId] = "user-7",
            [MessageHeaders.TenantId] = "tenant-7",
            [MessageHeaders.CustomPrefix + "region"] = "east",
            [AzureServiceBusHeaders.Attempt] = 2,
            [AzureServiceBusHeaders.TargetSubscription] = "billing",
            [AzureServiceBusHeaders.EventType] = "order-placed",
        };

        var envelope = MessageEnvelopeHeaders.Read(
            AzureServiceBusHeaders.ToEnvelope(properties),
            AzureServiceBusMessageTransport.TransportTypeName);

        Assert.Equal("user-7", envelope.Context.UserId);
        Assert.Equal("tenant-7", envelope.Context.TenantId);
        Assert.Equal("east", envelope.Context.Headers["region"]);
        Assert.False(envelope.Context.Headers.ContainsKey(AzureServiceBusHeaders.Attempt));
        Assert.False(envelope.Context.Headers.ContainsKey(AzureServiceBusHeaders.TargetSubscription));
        Assert.False(envelope.Context.Headers.ContainsKey(AzureServiceBusHeaders.EventType));
        Assert.Equal(2, AzureServiceBusHeaders.CompletedAttempts(properties));
    }

    [Fact]
    public void An_invalid_topic_name_is_rejected_before_publish_is_recorded()
    {
        var durations = 0L;
        var errors = 0L;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == "LawnDart.Messaging" &&
                instrument.Name is "messaging.publish.duration" or "messaging.publish.errors")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((instrument, _, _, _) =>
        {
            if (instrument.Name == "messaging.publish.duration")
                Interlocked.Increment(ref durations);
        });
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == "messaging.publish.errors")
                Interlocked.Add(ref errors, value);
        });
        listener.Start();

        using var transport = new AzureServiceBusMessageTransport(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<AzureServiceBusMessageTransport>.Instance,
            Microsoft.Extensions.Options.Options.Create(new AzureServiceBusTransportOptions
            {
                ConnectionString = "Endpoint=sb://127.0.0.1:1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE",
                OperationTimeout = TimeSpan.FromSeconds(1),
            }));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            transport.PublishAsync(new OddNamePing(Guid.NewGuid(), DateTime.UtcNow), MessageContext.New()).GetAwaiter().GetResult());
        Assert.Contains("topic", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, Volatile.Read(ref durations));
        Assert.Equal(0, Volatile.Read(ref errors));
    }

    [EventTypeName("not a topic")]
    private sealed record OddNamePing(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class FakeCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
            => new("token", DateTimeOffset.UtcNow.AddHours(1));

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
            => new(GetToken(requestContext, cancellationToken));
    }
}
