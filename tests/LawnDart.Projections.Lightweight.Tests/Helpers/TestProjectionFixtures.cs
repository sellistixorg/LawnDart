using LawnDart;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Sdk;

namespace LawnDart.Projections.Lightweight.Tests.Helpers;

// ── Minimal event types ────────────────────────────────────────────────────────

public record CounterIncremented(Guid Id, DateTime Timestamp, int Amount) : IEvent;
public record CounterReset(Guid Id, DateTime Timestamp) : IEvent;
public record GlobalTagged(Guid Id, DateTime Timestamp, string Tag) : IEvent;

/// <summary>
/// Emitted on an <c>OrderAggregate</c> stream — simulates order placement across tenants.
/// </summary>
public record OrderCreatedLocal(Guid Id, DateTime Timestamp, Guid OrderId, string Customer) : IEvent;

/// <summary>
/// Emitted on a <c>ShipmentAggregate</c> stream — a different stream type to the order stream,
/// used to verify the multi-stream projection combines events from two distinct stream types.
/// </summary>
public record ShipmentDispatchedLocal(Guid Id, DateTime Timestamp, Guid OrderId, string Carrier) : IEvent;

/// <summary>Item added to a listing aggregate stream.</summary>
public record CatalogItemAdded(Guid Id, DateTime Timestamp, Guid ListingId, Guid ItemId) : IEvent;

/// <summary>Item removed from a listing aggregate stream.</summary>
public record CatalogItemRemoved(Guid Id, DateTime Timestamp, Guid ListingId, Guid ItemId) : IEvent;

/// <summary>Matching event that a poison projection handles by throwing.</summary>
public record PoisonPill(Guid Id, DateTime Timestamp) : IEvent;

// ── Minimal view types ─────────────────────────────────────────────────────────

public class CounterView
{
    public int Count { get; set; }
    public string StreamId { get; set; } = string.Empty;
    public DateTime LastUpdated { get; set; }
}

public class GlobalIndexView
{
    public int TotalTags { get; set; }
    public List<string> Tags { get; set; } = [];
}

/// <summary>View produced by <see cref="OrderFulfillmentSummaryProjection"/>.</summary>
public class FulfillmentView
{
    public Guid OrderId { get; set; }
    public string Customer { get; set; } = string.Empty;
    public string? Carrier { get; set; }
    public bool IsShipped => Carrier is not null;
}

/// <summary>View for <see cref="CatalogListingProjection"/> — tracks item membership.</summary>
public class CatalogListingView
{
    public Guid ListingId { get; set; }
    public List<Guid> ItemIds { get; set; } = [];
    public int ProcessedEventCount { get; set; }
}

// ── Projection fixtures ────────────────────────────────────────────────────────

/// <summary>
/// A minimal single-stream projection used in scanner and runner tests.
/// </summary>
[SingleStreamProjection("CounterSummary", streamType: "Counter")]
[ProjectionEndpoint(
    route: "/api/views/counters/{counterId}",
    requiredPermission: "Counter.View")]
public class CounterSummaryProjection : ProjectionBase<CounterView>
{
    public void Handle(CounterIncremented e)
    {
        State.Count += e.Amount;
        State.StreamId = StreamId;
        State.LastUpdated = e.Timestamp;
    }

    public void Handle(CounterReset _)
    {
        State.Count = 0;
        State.LastUpdated = DateTime.UtcNow;
    }
}

/// <summary>
/// Last-write-wins counter for Matrix A crash/restart tests where views may sit ahead of the
/// global checkpoint and events are re-applied (additive handlers would double-count).
/// </summary>
[SingleStreamProjection("SetCountCounter", streamType: "Counter")]
public class SetCountCounterProjection : ProjectionBase<CounterView>
{
    public void Handle(CounterIncremented e)
    {
        State.Count = e.Amount;
        State.StreamId = StreamId;
        State.LastUpdated = e.Timestamp;
    }
}

/// <summary>
/// Version 1 of a logical projection family used to test explicit latest resolution.
/// </summary>
[SingleStreamProjection("VersionedCounterSummary", streamType: "Counter", Version = 1)]
public class VersionedCounterSummaryV1Projection : ProjectionBase<CounterView>
{
    public void Handle(CounterIncremented e) => State.Count += e.Amount;
}

/// <summary>
/// Version 2 of a logical projection family used to test explicit latest resolution.
/// </summary>
[SingleStreamProjection("VersionedCounterSummary", streamType: "Counter", Version = 2, IsLatest = true)]
public class VersionedCounterSummaryV2Projection : ProjectionBase<CounterView>
{
    public void Handle(CounterIncremented e) => State.Count += e.Amount * 2;
}

/// <summary>
/// Version 1 of a logical projection family used to test highest-version fallback resolution.
/// </summary>
[SingleStreamProjection("FallbackCounterSummary", streamType: "Counter", Version = 1)]
public class FallbackCounterSummaryV1Projection : ProjectionBase<CounterView>
{
    public void Handle(CounterIncremented e) => State.Count += e.Amount;
}

/// <summary>
/// Version 2 of a logical projection family used to test highest-version fallback resolution.
/// </summary>
[SingleStreamProjection("FallbackCounterSummary", streamType: "Counter", Version = 2)]
public class FallbackCounterSummaryV2Projection : ProjectionBase<CounterView>
{
    public void Handle(CounterIncremented e) => State.Count += e.Amount;
}

/// <summary>
/// Version 1 of a logical projection family used to test duplicate latest validation.
/// </summary>
[GlobalProjection("DuplicateLatestProjection", tenantScope: TenantScope.SystemGlobal, Version = 1, IsLatest = true)]
internal class DuplicateLatestProjectionV1 : ProjectionBase<GlobalIndexView>
{
    public void Handle(GlobalTagged e) => State.TotalTags++;
}

/// <summary>
/// Version 2 of a logical projection family used to test duplicate latest validation.
/// </summary>
[GlobalProjection("DuplicateLatestProjection", tenantScope: TenantScope.SystemGlobal, Version = 2, IsLatest = true)]
internal class DuplicateLatestProjectionV2 : ProjectionBase<GlobalIndexView>
{
    public void Handle(GlobalTagged e) => State.TotalTags++;
}

/// <summary>
/// A minimal global projection used in scanner and runner tests.
/// </summary>
[GlobalProjection("GlobalTagIndex", tenantScope: TenantScope.SystemGlobal)]
[ProjectionEndpoint(route: "/api/views/tags")]
public class GlobalTagIndexProjection : ProjectionBase<GlobalIndexView>
{
    public void Handle(GlobalTagged e)
    {
        State.TotalTags++;
        State.Tags.Add(e.Tag);
    }
}

/// <summary>
/// Global projection used to prove poison <c>Handle</c> exceptions retry then halt
/// without advancing the checkpoint.
/// </summary>
[GlobalProjection("PoisonTagIndex", tenantScope: TenantScope.SystemGlobal)]
public class PoisonTagIndexProjection : ProjectionBase<GlobalIndexView>
{
    public static int PoisonHandleCalls;

    public void Handle(GlobalTagged e)
    {
        State.TotalTags++;
        State.Tags.Add(e.Tag);
    }

    public void Handle(PoisonPill _)
    {
        Interlocked.Increment(ref PoisonHandleCalls);
        State.TotalTags++;
        throw new InvalidOperationException("poison");
    }
}

/// <summary>
/// A list-backed global projection used to verify top-level array state and diff behavior.
/// </summary>
[GlobalProjection("GlobalTagList", tenantScope: TenantScope.SystemGlobal)]
public class GlobalTagListProjection : ProjectionBase<List<string>>
{
    public void Handle(GlobalTagged e) => State.Add(e.Tag);
}

/// <summary>
/// A projection without a <see cref="ProjectionEndpointAttribute"/> — endpoint mapper should skip it.
/// </summary>
[SingleStreamProjection("NoEndpointProjection", streamType: "Widget", tenantScope: TenantScope.TenantGlobal)]
public class NoEndpointProjection : ProjectionBase<CounterView>
{
    public void Handle(CounterIncremented e) => State.Count += e.Amount;
}

/// <summary>
/// A DCB projection fixture.
/// </summary>
[DcbProjection("CounterDcbFeed", "CounterIncremented")]
public class CounterDcbFeedProjection : ProjectionBase<GlobalIndexView>
{
    public void Handle(CounterIncremented e) => State.TotalTags += e.Amount;
}

/// <summary>
/// Multi-stream projection fixture. Combines <see cref="OrderCreatedLocal"/> events (from an
/// <c>OrderAggregate</c> stream) with <see cref="ShipmentDispatchedLocal"/> events (from a
/// <c>ShipmentAggregate</c> stream), linked by the shared <c>OrderId</c>.
/// </summary>
[MultiStreamProjection("OrderFulfillmentSummary",
    typeof(OrderCreatedLocal),
    typeof(ShipmentDispatchedLocal))]
[ProjectionEndpoint(route: "/api/views/fulfillment/{orderId}")]
public class OrderFulfillmentSummaryProjection
    : ProjectionBase<FulfillmentView>, IMultiStreamEntityResolver
{
    public string? GetEntityId(IEvent @event) => @event switch
    {
        OrderCreatedLocal    e => e.OrderId.ToString(),
        ShipmentDispatchedLocal e => e.OrderId.ToString(),
        _                        => null
    };

    public void Handle(OrderCreatedLocal e)
    {
        State.OrderId  = e.OrderId;
        State.Customer = e.Customer;
    }

    public void Handle(ShipmentDispatchedLocal e)
    {
        State.Carrier = e.Carrier;
    }
}

/// <summary>
/// Multi-stream listing index: combines add/remove events that may arrive on different streams.
/// </summary>
[MultiStreamProjection("CatalogListing",
    typeof(CatalogItemAdded),
    typeof(CatalogItemRemoved))]
[ProjectionEndpoint(route: "/api/views/catalog/{listingId}")]
public class CatalogListingProjection
    : ProjectionBase<CatalogListingView>, IMultiStreamEntityResolver
{
    public string? GetEntityId(IEvent @event) => @event switch
    {
        CatalogItemAdded   e => e.ListingId.ToString(),
        CatalogItemRemoved e => e.ListingId.ToString(),
        _                    => null
    };

    public void Handle(CatalogItemAdded e)
    {
        State.ListingId = e.ListingId;
        if (!State.ItemIds.Contains(e.ItemId))
            State.ItemIds.Add(e.ItemId);
        State.ProcessedEventCount++;
    }

    public void Handle(CatalogItemRemoved e)
    {
        State.ListingId = e.ListingId;
        State.ItemIds.Remove(e.ItemId);
        State.ProcessedEventCount++;
    }
}

/// <summary>
/// A multi-stream projection that does NOT implement <see cref="IMultiStreamEntityResolver"/>.
/// Used to verify that the scanner throws an informative error.
/// </summary>
[MultiStreamProjection("BrokenMultiStream", typeof(CounterIncremented))]
internal class BrokenMultiStreamProjection : ProjectionBase<CounterView>
{
    // Intentionally missing IMultiStreamEntityResolver — scanner must reject this
    public void Handle(CounterIncremented e) => State.Count += e.Amount;
}

/// <summary>
/// A class that is NOT a projection — used to verify the scanner ignores it.
/// </summary>
public class NotAProjection
{
    public int Value { get; set; }
}
