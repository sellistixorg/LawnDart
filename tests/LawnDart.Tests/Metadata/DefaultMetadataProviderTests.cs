using System.Diagnostics;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Metadata;
using Xunit;

namespace LawnDart.Tests.Metadata;

public class DefaultMetadataProviderTests
{
    [Fact]
    public void CaptureCommandMetadata_ReturnsMetadataWithDefaults()
    {
        var provider = new DefaultMetadataProvider();

        var metadata = provider.CaptureCommandMetadata();

        Assert.NotNull(metadata);
        Assert.NotNull(metadata.CorrelationId);
        Assert.Null(metadata.TraceId);
        Assert.Null(metadata.SpanId);
        Assert.True(metadata.Timestamp <= DateTime.UtcNow);
    }

    [Fact]
    public void CaptureCommandMetadata_WithoutActivity_MintsCorrelationId()
    {
        var first = new DefaultMetadataProvider().CaptureCommandMetadata();
        var second = new DefaultMetadataProvider().CaptureCommandMetadata();

        Assert.False(string.IsNullOrWhiteSpace(first.CorrelationId));
        Assert.False(string.IsNullOrWhiteSpace(second.CorrelationId));
        Assert.NotEqual(first.CorrelationId, second.CorrelationId);
    }

    [Fact]
    public void CaptureCommandMetadata_WithActivity_SetsTraceAndCorrelation()
    {
        using var listener = EnableAllActivities();
        using var source = new ActivitySource("lawndart-w4-capture");
        using var activity = source.StartActivity("op");
        Assert.NotNull(activity);

        var metadata = new DefaultMetadataProvider().CaptureCommandMetadata();

        Assert.Equal(activity.TraceId.ToHexString(), metadata.TraceId);
        Assert.Equal(activity.SpanId.ToHexString(), metadata.SpanId);
        Assert.Equal(activity.TraceId.ToHexString(), metadata.CorrelationId);
    }

    [Fact]
    public void CaptureCommandMetadata_AmbientMessageContext_CopiesEnvelopeFields()
    {
        var inbound = new MessageContext
        {
            CorrelationId = "saga-1",
            CausationId = "cause-9",
            TenantId = "tenant-a",
            UserId = "user-b",
            Headers = new Dictionary<string, string>
            {
                ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
            }
        };

        using (AmbientMessageContext.Push(inbound))
        {
            var metadata = new DefaultMetadataProvider().CaptureCommandMetadata();

            Assert.Equal("saga-1", metadata.CorrelationId);
            Assert.Equal("cause-9", metadata.CausationId);
            Assert.Equal("tenant-a", metadata.TenantId);
            Assert.Equal("user-b", metadata.UserId);
            Assert.Equal("0af7651916cd43dd8448eb211c80319c", metadata.TraceId);
            Assert.Equal("b7ad6b7169203331", metadata.SpanId);
        }

        Assert.Null(AmbientMessageContext.Current);
    }

    [Fact]
    public void CaptureCommandMetadata_KeepsExistingCorrelation_WhenActivityPresent()
    {
        using var listener = EnableAllActivities();
        using var source = new ActivitySource("lawndart-w4-keep-corr");
        using var activity = source.StartActivity("op");
        Assert.NotNull(activity);

        var inbound = new MessageContext { CorrelationId = "already-set" };
        using (AmbientMessageContext.Push(inbound))
        {
            var metadata = new DefaultMetadataProvider().CaptureCommandMetadata();
            Assert.Equal("already-set", metadata.CorrelationId);
            Assert.Equal(activity.TraceId.ToHexString(), metadata.TraceId);
        }
    }

    private static ActivityListener EnableAllActivities()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static _ => true,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    [Fact]
    public void EnrichEventMetadata_InheritsFromCommandMetadata()
    {
        var provider = new DefaultMetadataProvider();
        var businessTime = new DateTime(2020, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var commandMetadata = new CommandMetadata
        {
            UserId = "user123",
            TenantId = "tenant456",
            CorrelationId = "corr-123",
            TraceId = "0af7651916cd43dd8448eb211c80319c",
            SpanId = "b7ad6b7169203331",
            Timestamp = DateTime.UtcNow
        };
        var @event = new TestEvent(Guid.NewGuid(), businessTime);
        var baseMetadata = new EventMetadata();

        var enriched = provider.EnrichEventMetadata(baseMetadata, commandMetadata, @event);

        Assert.Equal("user123", enriched.UserId);
        Assert.Equal("tenant456", enriched.TenantId);
        Assert.Equal("corr-123", enriched.CorrelationId);
        Assert.Equal("corr-123", enriched.CausationId);
        Assert.Equal(@event.Id.ToString(), enriched.EventId);
        Assert.Equal(businessTime, enriched.Timestamp);
        Assert.Null(enriched.CommitTimestamp);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", enriched.TraceId);
        Assert.Equal("b7ad6b7169203331", enriched.SpanId);
        Assert.Equal("default-metadata-provider-tests.test-event", enriched.SchemaName);
        Assert.NotEqual(@event.GetType().FullName, enriched.SchemaName);
    }

    [Fact]
    public void EnrichEventMetadata_UsesEventTimestamp_NotUtcNow()
    {
        var provider = new DefaultMetadataProvider();
        var businessTime = new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var @event = new TestEvent(Guid.NewGuid(), businessTime);

        var enriched = provider.EnrichEventMetadata(
            new EventMetadata(),
            new CommandMetadata(),
            @event);

        Assert.Equal(businessTime, enriched.Timestamp);
        Assert.NotEqual(DateTime.UtcNow.Date, enriched.Timestamp.Date);
    }

    [EventTypeName("default-metadata-provider-tests.test-event")]
    private record TestEvent(Guid Id, DateTime Timestamp) : IEvent;
}


