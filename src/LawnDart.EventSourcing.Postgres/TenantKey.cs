namespace LawnDart.EventSourcing.Postgres;

/// <summary>
/// Storage tenant key. The same rule on every write and every read.
/// </summary>
/// <remarks>
/// An empty string is never stored. When the stream id has 3 or more
/// colon-separated parts and the first part is not empty, the key is that
/// first part. Otherwise the key is <c>default</c>. The public stream
/// metadata still derives its tenant from the stream id, so a stream
/// without a tenant prefix does not report <c>default</c>.
/// </remarks>
internal static class TenantKey
{
    /// <summary>Reserved storage value for a stream that has no tenant prefix.</summary>
    internal const string DefaultTenant = "default";

    /// <summary>Storage key for <paramref name="streamId"/>.</summary>
    internal static string ForStream(string streamId)
    {
        var parts = streamId.Split(':');
        if (parts.Length >= 3 && parts[0].Length > 0)
            return parts[0];
        return DefaultTenant;
    }

    /// <summary>
    /// Tenant fixed by a stream id prefix, when every match must share it.
    /// A prefix with 3 or more colon-separated parts and a non-empty first
    /// part qualifies. A shorter prefix can match both a tenant stream and
    /// a stream stored under <see cref="DefaultTenant"/>.
    /// </summary>
    internal static string? ForPrefix(string prefix)
    {
        var parts = prefix.Split(':');
        if (parts.Length >= 3 && parts[0].Length > 0)
            return parts[0];
        return null;
    }
}
