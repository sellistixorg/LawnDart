using LawnDart.Demo.Academy.Domain.CourseSection;
using LawnDart.Demo.Academy.Domain.CourseSection.Commands;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;
using LawnDart.Aggregates;
using LawnDart.Demo.Academy.Projections;
using LawnDart.EventStore;

namespace LawnDart.Demo.Academy.Showcases;

/// <summary>
/// Showcase C: Live Projection Panel — concurrent seat reservation with real-time dashboard.
///
/// Spawns N concurrent enrollment attempts, each trying to reserve a seat on
/// the same section. The CourseSectionProjector is updated as events are appended
/// and printed as a live dashboard every 200ms.
///
/// This demonstrates:
/// - The projection engine under realistic concurrency
/// - Optimistic concurrency failure handling (last N students get "no seats" error)
/// - The difference in projection lag between poll-mode and event-mode backends
/// </summary>
public class ShowcaseC_LiveProjectionPanel
{
    private readonly IAggregateRepository _repository;
    private readonly CourseSectionProjector _projector;

    public ShowcaseC_LiveProjectionPanel(
        IAggregateRepository repository,
        CourseSectionProjector projector)
    {
        _repository = repository;
        _projector = projector;
    }

    public async Task RunAsync(int totalStudents = 10, int totalSeats = 5)
    {
        Console.WriteLine($"\n{'─',72}");
        Console.WriteLine(" Showcase C: Live Projection Panel — Concurrent Seat Reservations");
        Console.WriteLine($"{'─',72}");
        Console.WriteLine($" {totalStudents} students racing for {totalSeats} seats. Dashboard refreshes every 200ms.");
        Console.WriteLine();

        var courseId = Guid.NewGuid();
        var sectionId = Guid.NewGuid();

        // Create and seed section
        var section = await _repository.GetOrCreateAsync<CourseSection>(sectionId);
        await _repository.HandleCommandAsync(section,
            new CreateSectionCommand(Guid.NewGuid(), sectionId, courseId, "Live Demo Section", totalSeats));
        _projector.Apply(new SectionCreated(Guid.NewGuid(), DateTime.UtcNow, sectionId, courseId, "Live Demo Section", totalSeats));

        Console.WriteLine($" Section created: {section.State.Title} — {section.State.TotalSeats} total seats\n");

        var successCount = 0;
        var failureCount = 0;
        var resultsLock = new Lock();

        // Launch N concurrent enrollment tasks
        var tasks = Enumerable.Range(1, totalStudents).Select(async i =>
        {
            var studentId = Guid.NewGuid();
            try
            {
                // Each student loads its own copy of the section and tries to reserve
                var s = await _repository.GetOrCreateAsync<CourseSection>(sectionId);
                await _repository.HandleCommandAsync(s,
                    new ReserveSeatCommand(Guid.NewGuid(), sectionId, studentId));

                _projector.Apply(new SeatReserved(Guid.NewGuid(), DateTime.UtcNow, sectionId, studentId));

                lock (resultsLock) successCount++;
                return (i, Success: true, Message: "Seat reserved");
            }
            catch (LawnDart.EventStore.ConcurrencyException)
            {
                // Lost the race — another student grabbed the last seat(s) concurrently.
                lock (resultsLock) failureCount++;
                return (i, Success: false, Message: "No seats available (lost optimistic concurrency race)");
            }
            catch (InvalidOperationException ex)
            {
                lock (resultsLock) failureCount++;
                return (i, Success: false, Message: ex.Message);
            }
        }).ToList();

        // Dashboard refresh loop
        var dashboardTask = Task.Run(async () =>
        {
            var completed = 0;
            while (completed < totalStudents)
            {
                var view = _projector.Get(sectionId);
                if (view is not null)
                {
                    Console.Write($"\r Course: {view.SectionTitle,-20} | Available: {view.SeatsAvailable,2}/{view.TotalSeats,2} | Reserved: {view.SeatsReserved,2}");
                }
                await Task.Delay(200);
                lock (resultsLock) completed = successCount + failureCount;
            }
        });

        var results = await Task.WhenAll(tasks);
        await dashboardTask;

        var finalView = _projector.Get(sectionId);
        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine($" ┌─────────────────────────────────────────────────────────┐");
        Console.WriteLine($" │  FINAL SEAT DASHBOARD                                   │");
        Console.WriteLine($" ├────────────────────────────┬────────────────────────────┤");
        Console.WriteLine($" │  Total seats               │  {totalSeats,-26}│");
        Console.WriteLine($" │  Successfully reserved     │  {successCount,-26}│");
        Console.WriteLine($" │  Rejected (no seats)       │  {failureCount,-26}│");
        Console.WriteLine($" │  Seats available (final)   │  {finalView?.SeatsAvailable ?? 0,-26}│");
        Console.WriteLine($" └────────────────────────────┴────────────────────────────┘");
        Console.WriteLine();

        foreach (var (i, success, message) in results.OrderBy(r => r.i))
        {
            var icon = success ? "✓" : "✗";
            Console.WriteLine($"   [{icon}] Student {i:D2}: {message}");
        }
    }
}
