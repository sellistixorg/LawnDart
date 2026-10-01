using LawnDart.Aggregates;
using LawnDart.Demo.Academy.Domain.Course;
using LawnDart.Demo.Academy.Domain.Course.Commands;
using LawnDart.Demo.Academy.Projections;
using LawnDart.EventStore;

namespace LawnDart.Demo.Academy.Showcases;

/// <summary>
/// Course catalog display. Creates a draft and a published course, folds their
/// streams into <see cref="CourseCatalogProjector"/>, and prints published rows.
/// </summary>
public class ShowcaseF_CourseCatalog
{
    private readonly IAggregateRepository _repository;
    private readonly IEventStore _eventStore;
    private readonly CourseCatalogProjector _projector;

    public ShowcaseF_CourseCatalog(
        IAggregateRepository repository,
        IEventStore eventStore,
        CourseCatalogProjector projector)
    {
        _repository = repository;
        _eventStore = eventStore;
        _projector = projector;
    }

    public async Task RunAsync()
    {
        var publishedId = Guid.NewGuid();
        var draftId = Guid.NewGuid();

        var published = await _repository.GetOrCreateAsync<Course>(publishedId);
        await _repository.HandleCommandAsync(published,
            new CreateCourseCommand(Guid.NewGuid(), publishedId, "Advanced Event Sourcing", "Master ES, CQRS, and DCB", 299m));
        await _repository.HandleCommandAsync(published,
            new PublishCourseCommand(Guid.NewGuid(), publishedId));
        await _repository.HandleCommandAsync(published,
            new UpdateCoursePriceCommand(Guid.NewGuid(), publishedId, 349m));

        var draft = await _repository.GetOrCreateAsync<Course>(draftId);
        await _repository.HandleCommandAsync(draft,
            new CreateCourseCommand(Guid.NewGuid(), draftId, "Draft Workshop", "Not yet published", 49m));

        await FoldAsync(published.StreamId);
        await FoldAsync(draft.StreamId);

        var catalog = _projector.GetPublished();
        var omitted = _projector.GetAll().Count(c => c.Status != CourseCatalogStatus.Published);

        Console.WriteLine(" Published courses:");
        Console.WriteLine();
        if (catalog.Count == 0)
            Console.WriteLine("   (none)");

        foreach (var course in catalog)
        {
            Console.WriteLine($"   {course.Title}");
            Console.WriteLine($"     {course.Description}");
            Console.WriteLine($"     £{course.Price:F2}  {course.Status}");
            Console.WriteLine();
        }

        Console.WriteLine($" Draft courses omitted: {omitted}");

        var row = catalog.SingleOrDefault(c => c.CourseId == publishedId);
        if (row is null
            || row.Title != "Advanced Event Sourcing"
            || row.Description != "Master ES, CQRS, and DCB"
            || row.Price != 349m
            || row.Status != CourseCatalogStatus.Published
            || catalog.Any(c => c.CourseId == draftId))
        {
            throw new InvalidOperationException(
                "Course catalog did not list the published course at £349 and omit the draft.");
        }
    }

    private async Task FoldAsync(string streamId)
    {
        var events = await _eventStore.ReadStreamAsync(streamId);
        foreach (var sequenced in events)
            _projector.Apply(sequenced.Event);
    }
}
