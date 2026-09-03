using LawnDart;
using LawnDart.Demo.Academy.Domain.Course;
using LawnDart.Demo.Academy.Domain.Course.Events;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Demo.Academy.Showcases;

/// <summary>
/// Showcase E: Temporal Queries and IAsyncEnumerable.
///
/// Demonstrates:
///   • ReadStreamAsync with toTimestamp — query state "as of" a point in time
///   • ReadStreamEnumerableAsync — stream events one-by-one (large streams)
///   • Course price history: create course, append price updates, query price at different times
/// </summary>
public class ShowcaseE_TemporalQueries
{
    private readonly IEventStore _eventStore;

    public ShowcaseE_TemporalQueries(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public async Task RunAsync()
    {
        var courseId = Guid.NewGuid();
        var streamId = $"academy-tenant:Course:{courseId}";

        // Timeline: t0=10:00, t1=10:05, t2=10:10, t3=10:15 (fixed dates for determinism)
        var t0 = new DateTime(2025, 6, 1, 10, 0, 0, DateTimeKind.Utc);
        var t1 = new DateTime(2025, 6, 1, 10, 5, 0, DateTimeKind.Utc);
        var t2 = new DateTime(2025, 6, 1, 10, 10, 0, DateTimeKind.Utc);
        var t3 = new DateTime(2025, 6, 1, 10, 15, 0, DateTimeKind.Utc);

        Console.WriteLine(" Creating course and appending price updates with explicit timestamps...\n");

        // Append all events directly with explicit timestamps for deterministic temporal demo
        await _eventStore.AppendAsync(
            streamId,
            [new CourseCreated(Guid.NewGuid(), t0, courseId, "Temporal Queries 101", "Learn point-in-time state", 299m)],
            expectedVersion: null,
            metadata: new EventMetadata { Timestamp = t0 });

        await _eventStore.AppendAsync(
            streamId,
            [new CoursePriceUpdated(Guid.NewGuid(), t1, courseId, 349m)],
            expectedVersion: 1,
            metadata: new EventMetadata { Timestamp = t1 });

        await _eventStore.AppendAsync(
            streamId,
            [new CoursePriceUpdated(Guid.NewGuid(), t2, courseId, 399m)],
            expectedVersion: 2,
            metadata: new EventMetadata { Timestamp = t2 });

        await _eventStore.AppendAsync(
            streamId,
            [new CoursePriceUpdated(Guid.NewGuid(), t3, courseId, 449m)],
            expectedVersion: 3,
            metadata: new EventMetadata { Timestamp = t3 });

        Console.WriteLine(" Timeline:");
        Console.WriteLine($"   t0 10:00  CourseCreated        → Price £299");
        Console.WriteLine($"   t1 10:05  CoursePriceUpdated   → Price £349");
        Console.WriteLine($"   t2 10:10  CoursePriceUpdated   → Price £399");
        Console.WriteLine($"   t3 10:15  CoursePriceUpdated   → Price £449");
        Console.WriteLine();

        // Step 2: Query state at different points in time using toTimestamp
        var stateAt10_07 = await RebuildStateAtAsync(streamId, new DateTime(2025, 6, 1, 10, 7, 0, DateTimeKind.Utc));
        var stateAt10_12 = await RebuildStateAtAsync(streamId, new DateTime(2025, 6, 1, 10, 12, 0, DateTimeKind.Utc));
        var stateAt10_20 = await RebuildStateAtAsync(streamId, new DateTime(2025, 6, 1, 10, 20, 0, DateTimeKind.Utc));

        Console.WriteLine(" Temporal queries (ReadStreamAsync with toTimestamp):");
        Console.WriteLine($"   As of 10:07  → Price £{stateAt10_07.Price}  (expect £349)");
        Console.WriteLine($"   As of 10:12  → Price £{stateAt10_12.Price}  (expect £399)");
        Console.WriteLine($"   As of 10:20  → Price £{stateAt10_20.Price}  (expect £449)");
        Console.WriteLine();

        // Step 3: Demonstrate ReadStreamEnumerableAsync
        Console.WriteLine(" Stream events via ReadStreamEnumerableAsync (await foreach):");
        var count = 0;
        await foreach (var evt in _eventStore.ReadStreamEnumerableAsync(streamId))
        {
            count++;
            var priceInfo = evt.Event is CoursePriceUpdated pu ? $" → £{pu.NewPrice}" : "";
            Console.WriteLine($"   [{count}] v{evt.Version} @ {evt.Metadata.Timestamp:HH:mm}  {evt.Event.GetType().Name}{priceInfo}");
        }
        Console.WriteLine($"   Total: {count} events\n");

        Console.WriteLine(" Key APIs:");
        Console.WriteLine("   • ReadStreamAsync(streamId, toTimestamp: asOf) — point-in-time read");
        Console.WriteLine("   • ReadStreamEnumerableAsync(streamId) — stream large event sets");
    }

    private async Task<CourseState> RebuildStateAtAsync(string streamId, DateTime asOf)
    {
        var events = await _eventStore.ReadStreamAsync(streamId, fromVersion: 0, toTimestamp: asOf);
        var state = new CourseState();

        foreach (var se in events.OrderBy(e => e.Version))
        {
            switch (se.Event)
            {
                case CourseCreated e:
                    state.CourseId = e.CourseId;
                    state.Title = e.Title;
                    state.Description = e.Description;
                    state.Price = e.Price;
                    break;
                case CoursePublished:
                    state.IsPublished = true;
                    break;
                case CoursePriceUpdated e:
                    state.Price = e.NewPrice;
                    break;
            }
        }

        return state;
    }
}
