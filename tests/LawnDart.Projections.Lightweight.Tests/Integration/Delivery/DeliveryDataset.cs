using LawnDart;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Sdk;

namespace LawnDart.Projections.Lightweight.Tests.Integration.Delivery;

/// <summary>Noise type that no delivery handler <c>Handle</c>s. Public so DCB name resolution can see it.</summary>
[EventTypeName("DeliveryNoiseAppended")]
public sealed record DeliveryNoiseAppended(Guid Id, DateTime Timestamp, int Slot) : IEvent;

/// <summary>Global-only match.</summary>
[EventTypeName("DeliveryGlobalMatched")]
public sealed record DeliveryGlobalMatched(Guid Id, DateTime Timestamp, int Slot) : IEvent;

/// <summary>DCB + SingleStream match (amount is always 1 so gold = count).</summary>
[EventTypeName("DeliveryCounterMatched")]
public sealed record DeliveryCounterMatched(Guid Id, DateTime Timestamp, int Amount, int StreamIndex) : IEvent;

internal sealed class DeliveryCountView
{
    public int Count { get; set; }
}

/// <summary>
/// Serializes classes that increment <see cref="DeliveryHandleCounters"/>.
/// Those counters are process-wide; xUnit otherwise runs the delivery classes in parallel
/// and gold asserts see another test's Handle calls (e.g. 500+500+30 = 1030).
/// </summary>
[CollectionDefinition(Name)]
public sealed class ProjectionDeliveryCounterCollection
{
    public const string Name = "ProjectionDeliveryCounters";
}

internal static class DeliveryHandleCounters
{
    public static long Global;
    public static long Dcb;
    public static long SingleStream;
    public static long ProcessEvent;

    public static long HandleTotal =>
        Interlocked.Read(ref Global) + Interlocked.Read(ref Dcb) + Interlocked.Read(ref SingleStream);

    public static void Reset()
    {
        Interlocked.Exchange(ref Global, 0);
        Interlocked.Exchange(ref Dcb, 0);
        Interlocked.Exchange(ref SingleStream, 0);
        Interlocked.Exchange(ref ProcessEvent, 0);
    }
}

[GlobalProjection("DeliveryGlobalCount", tenantScope: TenantScope.SystemGlobal)]
internal sealed class DeliveryGlobalCountProjection : ProjectionBase<DeliveryCountView>
{
    public override void ProcessEvent(IEvent @event, long sequencePosition)
    {
        Interlocked.Increment(ref DeliveryHandleCounters.ProcessEvent);
        base.ProcessEvent(@event, sequencePosition);
    }

    public void Handle(DeliveryGlobalMatched _)
    {
        Interlocked.Increment(ref DeliveryHandleCounters.Global);
        State.Count++;
    }
}

[DcbProjection("DeliveryDcbCount", "DeliveryCounterMatched")]
internal sealed class DeliveryDcbCountProjection : ProjectionBase<DeliveryCountView>
{
    public override void ProcessEvent(IEvent @event, long sequencePosition)
    {
        Interlocked.Increment(ref DeliveryHandleCounters.ProcessEvent);
        base.ProcessEvent(@event, sequencePosition);
    }

    public void Handle(DeliveryCounterMatched e)
    {
        Interlocked.Increment(ref DeliveryHandleCounters.Dcb);
        State.Count += e.Amount;
    }
}

[SingleStreamProjection("DeliveryStreamCount", streamType: "DeliveryCounter")]
internal sealed class DeliveryStreamCountProjection : ProjectionBase<DeliveryCountView>
{
    public override void ProcessEvent(IEvent @event, long sequencePosition)
    {
        Interlocked.Increment(ref DeliveryHandleCounters.ProcessEvent);
        base.ProcessEvent(@event, sequencePosition);
    }

    public void Handle(DeliveryCounterMatched e)
    {
        Interlocked.Increment(ref DeliveryHandleCounters.SingleStream);
        State.Count += e.Amount;
    }
}

/// <summary>
/// Fixed-seed Slice 0 dataset. Matching events are planted in the first third, middle third,
/// and last <see cref="TailBand"/> slots so later steps cannot “pass” by only scanning the tail.
/// </summary>
internal sealed record DeliverySliceSpec(
    int NoiseCount,
    int MatchCount,
    long Head,
    int GlobalMatches,
    int CounterMatches,
    IReadOnlyDictionary<string, int> SingleStreamGold,
    IReadOnlyList<(string StreamId, IEvent Event)> Events)
{
    public const int Seed = 20260818;
    public const int TailBand = 1000;
    public const int CounterStreamCount = 5;
    public const string NoiseStreamId = "tenant:DeliveryNoise:log";
    public const string GlobalStreamId = "system:DeliveryTags:1";

    public static string CounterStreamId(int index) => $"tenant:DeliveryCounter:{index}";

    public static DeliverySliceSpec Build(int noiseCount, int matchCount)
    {
        if (noiseCount < 0)
            throw new ArgumentOutOfRangeException(nameof(noiseCount));
        if (matchCount < 6)
            throw new ArgumentOutOfRangeException(nameof(matchCount), "Need enough matches for first/mid/tail bands.");

        var total = noiseCount + matchCount;
        var matchSlots = PickMatchSlots(total, matchCount);
        var globalMatchCount = matchCount / 2;
        var counterMatchCount = matchCount - globalMatchCount;

        var events = new (string StreamId, IEvent Event)[total];
        var now = new DateTime(2026, 8, 18, 0, 0, 0, DateTimeKind.Utc);
        var singleStreamGold = Enumerable.Range(0, CounterStreamCount)
            .ToDictionary(CounterStreamId, _ => 0);

        var matchOrdinal = 0;
        for (var slot = 0; slot < total; slot++)
        {
            if (!matchSlots.Contains(slot))
            {
                events[slot] = (NoiseStreamId, new DeliveryNoiseAppended(GuidFromSlot(slot), now, slot));
                continue;
            }

            if (matchOrdinal < globalMatchCount)
            {
                events[slot] = (GlobalStreamId, new DeliveryGlobalMatched(GuidFromSlot(slot), now, slot));
            }
            else
            {
                var streamIndex = (matchOrdinal - globalMatchCount) % CounterStreamCount;
                var streamId = CounterStreamId(streamIndex);
                events[slot] = (streamId, new DeliveryCounterMatched(GuidFromSlot(slot), now, Amount: 1, streamIndex));
                singleStreamGold[streamId]++;
            }

            matchOrdinal++;
        }

        return new DeliverySliceSpec(
            noiseCount,
            matchCount,
            Head: total,
            GlobalMatches: globalMatchCount,
            CounterMatches: counterMatchCount,
            singleStreamGold,
            events);
    }

    private static HashSet<int> PickMatchSlots(int total, int matchCount)
    {
        var rng = new Random(Seed);
        var slots = new HashSet<int>();
        var firstEnd = Math.Max(1, total / 3);
        var midStart = firstEnd;
        var midEnd = Math.Max(midStart + 1, (2 * total) / 3);
        var tailStart = Math.Max(0, total - TailBand);

        void TakeFrom(int start, int exclusiveEnd, int n)
        {
            var span = exclusiveEnd - start;
            if (span <= 0 || n <= 0)
                return;
            var guard = 0;
            while (slots.Count < matchCount && n > 0 && guard++ < span * 8)
            {
                if (slots.Add(rng.Next(start, exclusiveEnd)))
                    n--;
            }
        }

        // Guarantee sparse coverage: at least two matches in each band.
        TakeFrom(0, firstEnd, 2);
        TakeFrom(midStart, midEnd, 2);
        TakeFrom(tailStart, total, 2);
        TakeFrom(0, total, matchCount - slots.Count);

        if (slots.Count != matchCount)
            throw new InvalidOperationException($"Expected {matchCount} match slots, got {slots.Count}.");

        if (!slots.Any(s => s < firstEnd) ||
            !slots.Any(s => s >= midStart && s < midEnd) ||
            !slots.Any(s => s >= tailStart))
            throw new InvalidOperationException("Match slots missing a required first/mid/tail band.");

        return slots;
    }

    private static Guid GuidFromSlot(int slot)
    {
        var bytes = new byte[16];
        BitConverter.GetBytes(Seed).CopyTo(bytes, 0);
        BitConverter.GetBytes(slot).CopyTo(bytes, 4);
        return new Guid(bytes);
    }
}
