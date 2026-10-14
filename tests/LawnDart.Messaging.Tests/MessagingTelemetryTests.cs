using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;
using LawnDart.Messaging.Telemetry;

namespace LawnDart.Messaging.Tests;

public class MessagingTelemetryTests
{
    [Fact]
    public async Task Retry_and_dead_letter_record_counters_logs_and_span_events()
    {
        var retries = 0L;
        var dead = 0L;
        using var meters = new MeterListener();
        meters.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name != MessagingTelemetry.MeterName)
                return;
            if (instrument.Name is "messaging.delivery.retries" or "messaging.deadlettered")
                listener.EnableMeasurementEvents(instrument);
        };
        meters.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            if (!HasTag(tags, "messaging.message_type", "messaging.named-ping") ||
                !HasTag(tags, "messaging.subscription_name", "telemetry-sub"))
                return;

            if (instrument.Name == "messaging.delivery.retries")
                Interlocked.Add(ref retries, value);
            else if (instrument.Name == "messaging.deadlettered")
                Interlocked.Add(ref dead, value);
        });
        meters.Start();

        using var activitySource = new ActivitySource("telemetry-delivery-test");
        using var activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name == activitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(activities);

        var logger = new CaptureLogger();
        var options = new MessageTransportOptions
        {
            SubscriptionName = "telemetry-sub",
            MaxDeliveryAttempts = 3,
            DeadLetterFailedMessages = true,
        };

        using (var activity = activitySource.StartActivity("deliver"))
        {
            await MessageDelivery.DeliverAsync(
                new NamedPing(Guid.NewGuid(), DateTime.UtcNow),
                new MessageContext { MessageId = "msg-tel" },
                (_, _, _) => throw new InvalidOperationException("again"),
                options,
                [],
                new MemoryDeadLetterQueue(),
                logger: logger);

            Assert.NotNull(activity);
            Assert.Equal(ActivityStatusCode.Error, activity.Status);
            Assert.Equal(2, activity.Events.Count(e => e.Name == "messaging.retry"));
            var parked = Assert.Single(activity.Events, e => e.Name == "messaging.deadlettered");
            Assert.Contains(parked.Tags, tag => tag.Key == "messaging.attempt" && tag.Value is 3);
            Assert.Contains(parked.Tags, tag => tag.Key == "error.type" && (string?)tag.Value == "InvalidOperationException");
        }

        Assert.Equal(2, Volatile.Read(ref retries));
        Assert.Equal(1, Volatile.Read(ref dead));
        Assert.Equal(2, logger.DebugCount);
        Assert.Contains("msg-tel", logger.Warning, StringComparison.Ordinal);
        Assert.Contains("messaging.named-ping", logger.Warning, StringComparison.Ordinal);
        Assert.Contains("3", logger.Warning, StringComparison.Ordinal);
        Assert.Contains("again", logger.Warning, StringComparison.Ordinal);
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

    [EventTypeName("messaging.named-ping")]
    private sealed record NamedPing(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class CaptureLogger : ILogger
    {
        public int DebugCount { get; private set; }
        public string Warning { get; private set; } = "";

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Debug)
                DebugCount++;
            if (logLevel == LogLevel.Warning)
                Warning = formatter(state, exception);
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }
}
