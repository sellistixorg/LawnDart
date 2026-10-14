using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.EventStore;
using LawnDart.Messaging.Telemetry;

namespace LawnDart.Messaging.RabbitMQ.Tests;

public class RabbitMqNameRejectionTests
{
    [Fact]
    public void An_invalid_exchange_name_is_rejected_before_publish_is_recorded()
    {
        var durations = 0L;
        var errors = 0L;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == MessagingTelemetry.MeterName &&
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

        using var transport = new RabbitMqMessageTransport(
            NullLogger<RabbitMqMessageTransport>.Instance,
            Options.Create(new RabbitMqTransportOptions
            {
                Uri = "amqp://rabbitmq:rabbitmq@127.0.0.1:1/",
                TopicMapper = new DelegateTopicMapper(_ => "amq.reserved"),
            }));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            transport.PublishAsync(new NamedPing(Guid.NewGuid(), DateTime.UtcNow), MessageContext.New()).GetAwaiter().GetResult());
        Assert.Contains("reserved", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, Volatile.Read(ref durations));
        Assert.Equal(0, Volatile.Read(ref errors));
    }

    [EventTypeName("messaging.rabbitmq.name-rejection")]
    private sealed record NamedPing(Guid Id, DateTime Timestamp) : IEvent;
}
