using System.Diagnostics;

namespace LawnDart.Messaging;

/// <summary>
/// Reads W3C <c>traceparent</c> / <c>tracestate</c> from <see cref="MessageContext.Headers"/>.
/// </summary>
internal static class W3CTraceParent
{
    public const string TraceParentHeader = "traceparent";
    public const string TraceStateHeader = "tracestate";

    public static bool TryGetActivityContext(
        IReadOnlyDictionary<string, string>? headers,
        out ActivityContext context)
    {
        context = default;
        if (headers is null || headers.Count == 0)
            return false;

        if (!TryGetHeader(headers, TraceParentHeader, out var traceparent) ||
            string.IsNullOrWhiteSpace(traceparent))
        {
            return false;
        }

        TryGetHeader(headers, TraceStateHeader, out var tracestate);
        return ActivityContext.TryParse(traceparent, tracestate, out context) && context != default;
    }

    public static bool TryGetIds(
        IReadOnlyDictionary<string, string>? headers,
        out string? traceId,
        out string? spanId)
    {
        if (!TryGetActivityContext(headers, out var context))
        {
            traceId = null;
            spanId = null;
            return false;
        }

        traceId = context.TraceId.ToHexString();
        spanId = context.SpanId.ToHexString();
        return true;
    }

    private static bool TryGetHeader(
        IReadOnlyDictionary<string, string> headers,
        string name,
        out string? value)
    {
        if (headers.TryGetValue(name, out value) && !string.IsNullOrWhiteSpace(value))
            return true;

        foreach (var pair in headers)
        {
            if (string.Equals(pair.Key, name, StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrWhiteSpace(pair.Value))
            {
                value = pair.Value;
                return true;
            }
        }

        value = null;
        return false;
    }
}
