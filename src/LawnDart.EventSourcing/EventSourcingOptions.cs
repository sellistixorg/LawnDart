namespace LawnDart.EventSourcing;

/// <summary>
/// Configuration options for event sourcing operations.
/// </summary>
public class EventSourcingOptions
{
    /// <summary>
    /// Enforce tenant isolation for DCB operations.
    /// When true, all DCB operations must include a tenant tag or have ITenantContextProvider available.
    /// When false, DCB operations can use any tags without tenant enforcement.
    /// Default: false
    /// </summary>
    public bool EnforceDcbTenantIsolation { get; set; } = false;

    /// <summary>
    /// How long a processor claim hides an outbox row from other processors.
    /// Default: 30 seconds.
    /// </summary>
    /// <remarks>
    /// SQL Server stores the deadline in <c>ClaimedUntil</c>, in whole seconds
    /// rounded up. Postgres stores <c>claimed_until</c> as a timestamp with the
    /// full time span. The in-memory writer uses the full time span.
    /// <see cref="TimeSpan.Zero"/> expires the claim at once, so the next poll
    /// can take the row.
    /// </remarks>
    public TimeSpan OutboxClaimLease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Log once per tag type, in this context, when a queried tag does not
    /// round-trip through the context <see cref="LawnDart.Tagging.ITagFormat"/>.
    /// </summary>
    /// <remarks>
    /// Default: true. The check covers Dynamic Consistency Boundary loads
    /// (<c>QueryItem.ByTags</c>), append fences (<c>AppendCondition.FailIfMatches</c>),
    /// and projector or reactor tags after <c>UseTagFormat</c>. Different ids of
    /// one type share one warning. After 128 tag types, further unknown tag
    /// warnings in that context are suppressed. It does not throw and does not
    /// change which events match. Set false to skip the log. A host with no
    /// <see cref="LawnDart.Tagging.ITagFormat"/> skips the check.
    /// </remarks>
    public bool WarnOnUnknownTag { get; set; } = true;

    /// <summary>
    /// When true, the aggregate fallback tag is built with the context
    /// <see cref="LawnDart.Tagging.ITagFormat"/> instead of <c>{ClrTypeName}:{id}</c>.
    /// </summary>
    /// <remarks>
    /// Default: false. The stored fallback stays <c>{ClrTypeName}:{id}</c>
    /// (<c>Student:{id}</c>). Leave this false on a store that already has events.
    /// Those events stay under the old tag. A query for the formatted tag does not
    /// see them. Set this only on a new store, where every fallback tag will use
    /// the format. With no format registered, the fallback uses
    /// <see cref="LawnDart.Tagging.LowercaseTagFormat"/>.
    /// </remarks>
    public bool FormatAggregateFallbackTag { get; set; } = false;
}
