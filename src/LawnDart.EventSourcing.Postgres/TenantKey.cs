namespace LawnDart.EventSourcing.Postgres;

/// <summary>
/// Storage tenant key. The same rule on every write and every read.
/// </summary>
/// <remarks>
/// An empty string is never stored. In <see cref="Tenancy.Single"/> with <c>SingleTenantId</c> set,
/// every row uses that fixed tenant id. Otherwise the tenant comes from
/// <see cref="LawnDart.EventStore.StreamIdText.TenantId"/> so <c>{tenant}:dcb</c> and
/// <c>{tenant}:{type}:{id}</c> use that tenant, and <c>dcb</c> or <c>{type}:{id}</c> use <c>default</c>.
/// The public stream metadata still derives its tenant from the stream id, so a stream without a
/// tenant prefix does not report <c>default</c>.
/// </remarks>
internal static class TenantKey
{
    /// <summary>Reserved storage value for a stream that has no tenant prefix.</summary>
    internal const string DefaultTenant = "default";

    /// <summary>Storage key for <paramref name="streamId"/>.</summary>
    internal static string ForStream(string streamId, Tenancy tenancy = Tenancy.Shared, string? singleTenantId = null)
        => tenancy == Tenancy.Single && singleTenantId is not null
            ? singleTenantId
            : LawnDart.EventStore.StreamIdText.TenantId(streamId) ?? DefaultTenant;

    /// <summary>
    /// Tenant fixed by a stream id prefix, when every match must share it.
    /// A prefix with 3 or more colon-separated parts and a non-empty first
    /// part qualifies. A shorter prefix can match both a tenant stream and
    /// a stream stored under <see cref="DefaultTenant"/>.
    /// </summary>
    internal static string? ForPrefix(string prefix, Tenancy tenancy = Tenancy.Shared, string? singleTenantId = null)
    {
        if (tenancy == Tenancy.Single && singleTenantId is not null)
            return singleTenantId;

        var parts = prefix.Split(':');
        if (parts.Length >= 3 && parts[0].Length > 0)
            return parts[0];
        return null;
    }
}
