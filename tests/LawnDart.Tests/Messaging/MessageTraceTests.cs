using System.Diagnostics;
using LawnDart.Messaging;
using LawnDart.Metadata;

namespace LawnDart.Tests.Messaging;

public class MessageTraceTests
{
    [Fact]
    public void WithMetadataTraceHeaders_AddsTraceIdSpanIdAndTraceparent()
    {
        var headers = MessageTrace.WithMetadataTraceHeaders(
            new Dictionary<string, string> { ["keep"] = "yes" },
            new EventMetadata
            {
                TraceId = "0af7651916cd43dd8448eb211c80319c",
                SpanId = "b7ad6b7169203331"
            });

        Assert.Equal("yes", headers["keep"]);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", headers["TraceId"]);
        Assert.Equal("b7ad6b7169203331", headers["SpanId"]);
        Assert.Equal(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            headers[MessageTrace.TraceParentHeader]);
    }

    [Fact]
    public void WithCurrentTraceHeaders_OverlaysActivityId()
    {
        using var listener = EnableAllActivities();
        using var source = new ActivitySource("lawndart-w5-trace");
        using var activity = source.StartActivity("op");
        Assert.NotNull(activity);

        var headers = MessageTrace.WithCurrentTraceHeaders(
            new Dictionary<string, string> { ["keep"] = "yes" },
            activity);

        Assert.Equal("yes", headers["keep"]);
        Assert.Equal(activity.Id, headers[MessageTrace.TraceParentHeader]);
    }

    [Fact]
    public void Start_ContinuesFromTraceparentAndTags()
    {
        using var listener = EnableAllActivities();
        using var source = new ActivitySource("lawndart-w5-start");
        var context = new MessageContext
        {
            CorrelationId = "saga-1",
            MessageId = "msg-9",
            Headers = new Dictionary<string, string>
            {
                [MessageTrace.TraceParentHeader] =
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
            }
        };

        using var activity = MessageTrace.Start(source, "handle", context);
        Assert.NotNull(activity);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", activity.TraceId.ToHexString());
        Assert.Equal("saga-1", activity.GetTagItem(MessageTrace.CorrelationIdTag));
        Assert.Equal("msg-9", activity.GetTagItem(MessageTrace.MessageIdTag));
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
}
