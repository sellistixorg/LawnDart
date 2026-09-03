using LawnDart;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Sdk;

namespace LawnDart.Projections.Lightweight.Tests.Integration.Delivery;

/// <summary>Homogeneous catch-up tick — public so DCB type-name resolution can see it.</summary>
[EventTypeName("CatchUpTick")]
public sealed record CatchUpTick(Guid Id, DateTime Timestamp, int Lane) : IEvent;

/// <summary>Shop-shaped type that is never appended in the catch-up cost log.</summary>
[EventTypeName("CatchUpShopNoise")]
public sealed record CatchUpShopNoise(Guid Id, DateTime Timestamp) : IEvent;

internal sealed class CatchUpTickView
{
    public long Ticks { get; set; }
    public DateTime LastUpdated { get; set; }
}

internal sealed class CatchUpNoopView
{
    public int Count { get; set; }
}

internal static class CatchUpCostCounters
{
    public static long Progress;
    public static long Noop;

    public static void Reset()
    {
        Interlocked.Exchange(ref Progress, 0);
        Interlocked.Exchange(ref Noop, 0);
    }
}

/// <summary>Counts ticks only — Chaos <c>CcPlaidProgress</c> analogue.</summary>
[DcbProjection("CatchUpProgress", "CatchUpTick")]
internal sealed class CatchUpProgressProjection : ProjectionBase<CatchUpTickView>
{
    public void Handle(CatchUpTick e)
    {
        Interlocked.Increment(ref CatchUpCostCounters.Progress);
        State.Ticks++;
        State.LastUpdated = e.Timestamp;
    }
}

[DcbProjection("CatchUpNoop1", "CatchUpShopNoise")]
internal sealed class CatchUpNoop1Projection : ProjectionBase<CatchUpNoopView>
{
    public void Handle(CatchUpShopNoise _)
    {
        Interlocked.Increment(ref CatchUpCostCounters.Noop);
        State.Count++;
    }
}

[DcbProjection("CatchUpNoop2", "CatchUpShopNoise")]
internal sealed class CatchUpNoop2Projection : ProjectionBase<CatchUpNoopView>
{
    public void Handle(CatchUpShopNoise _)
    {
        Interlocked.Increment(ref CatchUpCostCounters.Noop);
        State.Count++;
    }
}

[DcbProjection("CatchUpNoop3", "CatchUpShopNoise")]
internal sealed class CatchUpNoop3Projection : ProjectionBase<CatchUpNoopView>
{
    public void Handle(CatchUpShopNoise _)
    {
        Interlocked.Increment(ref CatchUpCostCounters.Noop);
        State.Count++;
    }
}

[DcbProjection("CatchUpNoop4", "CatchUpShopNoise")]
internal sealed class CatchUpNoop4Projection : ProjectionBase<CatchUpNoopView>
{
    public void Handle(CatchUpShopNoise _)
    {
        Interlocked.Increment(ref CatchUpCostCounters.Noop);
        State.Count++;
    }
}
