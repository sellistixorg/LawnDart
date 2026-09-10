using System.Diagnostics;
using LawnDart.Metadata;

namespace LawnDart.Messaging;

/// <summary>
/// Starts or continues a W3C <see cref="Activity"/> from <see cref="MessageContext.Headers"/>
/// and writes cheap correlation tags.
/// </summary>
public static class MessageTrace
{
    /// <summary>W3C traceparent header name.</summary>
    public const string TraceParentHeader = W3CTraceParent.TraceParentHeader;

    /// <summary>W3C tracestate header name.</summary>
    public const string TraceStateHeader = W3CTraceParent.TraceStateHeader;

    /// <summary>OTel tag for the saga / correlation id.</summary>
    public const string CorrelationIdTag = "correlation_id";

    /// <summary>OTel tag for the transport message id.</summary>
    public const string MessageIdTag = "messaging.message_id";

    /// <summary>
    /// Starts an activity, continuing from <c>traceparent</c> when present.
    /// Tags the started activity with correlation and message id when set.
    /// </summary>
    public static Activity? Start(
        ActivitySource source,
        string name,
        MessageContext? context,
        ActivityKind kind = ActivityKind.Consumer)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(name);

        Activity? activity;
        if (context is not null &&
            W3CTraceParent.TryGetActivityContext(context.Headers, out var parent) &&
            parent != default)
        {
            activity = source.StartActivity(name, kind, parent);
        }
        else
        {
            activity = source.StartActivity(name, kind);
        }

        Tag(activity, context);
        return activity;
    }

    /// <summary>Sets <see cref="CorrelationIdTag"/> and <see cref="MessageIdTag"/> when present.</summary>
    public static void Tag(Activity? activity, MessageContext? context)
    {
        if (activity is null || context is null)
            return;

        if (!string.IsNullOrWhiteSpace(context.CorrelationId))
            activity.SetTag(CorrelationIdTag, context.CorrelationId);
        if (!string.IsNullOrWhiteSpace(context.MessageId))
            activity.SetTag(MessageIdTag, context.MessageId);
    }

    /// <summary>
    /// Copies <paramref name="existing"/> and overlays the current (or given)
    /// activity's <c>traceparent</c> / <c>tracestate</c>.
    /// </summary>
    public static IReadOnlyDictionary<string, string> WithCurrentTraceHeaders(
        IReadOnlyDictionary<string, string>? existing,
        Activity? activity = null)
    {
        activity ??= Activity.Current;
        var dict = CopyHeaders(existing);

        if (activity is { IdFormat: ActivityIdFormat.W3C, Id: not null })
        {
            dict[TraceParentHeader] = activity.Id;
            if (!string.IsNullOrEmpty(activity.TraceStateString))
                dict[TraceStateHeader] = activity.TraceStateString;
        }

        return dict;
    }

    /// <summary>
    /// Copies <paramref name="existing"/> and adds <c>TraceId</c>, <c>SpanId</c>,
    /// and a formatted <c>traceparent</c> from <paramref name="metadata"/> when present.
    /// </summary>
    public static IReadOnlyDictionary<string, string> WithMetadataTraceHeaders(
        IReadOnlyDictionary<string, string>? existing,
        EventMetadata? metadata)
    {
        var dict = CopyHeaders(existing);
        if (metadata is null)
            return dict;

        if (!string.IsNullOrWhiteSpace(metadata.TraceId))
            dict["TraceId"] = metadata.TraceId;
        if (!string.IsNullOrWhiteSpace(metadata.SpanId))
            dict["SpanId"] = metadata.SpanId;

        if (!string.IsNullOrWhiteSpace(metadata.TraceId) &&
            !string.IsNullOrWhiteSpace(metadata.SpanId) &&
            metadata.TraceId.Length == 32 &&
            metadata.SpanId.Length == 16)
        {
            dict[TraceParentHeader] = $"00-{metadata.TraceId}-{metadata.SpanId}-01";
        }

        return dict;
    }

    private static Dictionary<string, string> CopyHeaders(IReadOnlyDictionary<string, string>? existing)
    {
        if (existing is null || existing.Count == 0)
            return new Dictionary<string, string>(StringComparer.Ordinal);

        return new Dictionary<string, string>(existing, StringComparer.Ordinal);
    }
}
