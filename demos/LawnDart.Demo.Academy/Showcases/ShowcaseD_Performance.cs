using MemoryPack;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;
using LawnDart.Metadata;

namespace LawnDart.Demo.Academy.Showcases;

/// <summary>
/// Showcase D: InMemory throughput demo.
///
/// Appends a batch of events as fast as possible using the in-process event store
/// and reports events/sec. Useful for understanding local ingest cost before
/// moving to SQL Server.
/// </summary>
public class ShowcaseD_Performance
{
    private readonly IEventStore _eventStore;

    public ShowcaseD_Performance(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public async Task RunAsync(int batchSize = 10_000)
    {
        Console.WriteLine($"\n{'─',72}");
        Console.WriteLine(" Showcase D: InMemory Throughput — High-Throughput Event Ingestion");
        Console.WriteLine($"{'─',72}");
        Console.WriteLine($" Appending {batchSize:N0} events in batches of 2000...\n");

        var streamId = $"academy-tenant:PerfDemo:{Guid.NewGuid():N}";
        const int innerBatchSize = 2000;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var appended = 0;

        var chunks = Enumerable.Range(0, batchSize / innerBatchSize)
            .Select(i => Enumerable.Range(i * innerBatchSize, innerBatchSize)
                .Select(seq => (IEvent)new GradeSubmitted(Guid.NewGuid(), DateTime.UtcNow, streamId, seq, 0.5 + (seq % 50) * 0.01))
                .ToArray())
            .ToList();

        foreach (var chunk in chunks)
        {
            await _eventStore.AppendAsync(
                streamId,
                chunk,
                expectedVersion: null,
                metadata: new EventMetadata { Timestamp = DateTime.UtcNow });
            appended += chunk.Length;

            if (appended % 2000 == 0)
                Console.Write($"\r   Appended {appended:N0}/{batchSize:N0}...");
        }

        sw.Stop();

        var throughput = batchSize / sw.Elapsed.TotalSeconds;
        var latencyPerEvent = sw.Elapsed.TotalMicroseconds / batchSize;

        Console.WriteLine($"\r   Appended {appended:N0}/{batchSize:N0} events.          ");
        Console.WriteLine();
        Console.WriteLine($" ┌─────────────────────────────────────────────────────────┐");
        Console.WriteLine($" │  PERFORMANCE RESULTS                                    │");
        Console.WriteLine($" ├────────────────────────────┬────────────────────────────┤");
        Console.WriteLine($" │  Events appended           │  {batchSize.ToString("N0"),-26}│");
        Console.WriteLine($" │  Total time                │  {$"{sw.Elapsed.TotalMilliseconds:N0} ms",-26}│");
        Console.WriteLine($" │  Throughput                │  {$"{throughput:N0} events/sec",-26}│");
        Console.WriteLine($" │  Avg latency per event     │  {$"{latencyPerEvent:N1} µs",-26}│");
        Console.WriteLine($" └────────────────────────────┴────────────────────────────┘");

        Console.WriteLine($"\n   Backend: {_eventStore.GetType().Name}");
        Console.WriteLine("   Note: InMemory is process-local — no network, no durable I/O.");
        Console.WriteLine("   For durable ingest, switch the launch profile to SQL Server.\n");
    }
}

/// <summary>Event used in the performance demonstration — simulates a grade submission.</summary>
[MemoryPackable]
[EventTypeName("GradeSubmitted")]
public partial record GradeSubmitted(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] string CourseStreamId,
    [property: MemoryPackOrder(3)] int SequenceNumber,
    [property: MemoryPackOrder(4)] double Score) : IEvent;
