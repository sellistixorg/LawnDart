using LawnDart;
using LawnDart.Demo.Academy.Domain.Course.Events;

namespace LawnDart.Demo.Academy.Projections;

/// <summary>
/// Lifecycle shown on a course catalog row.
/// </summary>
public enum CourseCatalogStatus
{
    Draft,
    Published,
    Archived
}

/// <summary>
/// One course in the catalog read model.
/// </summary>
public class CourseCatalogEntry
{
    public Guid CourseId { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public CourseCatalogStatus Status { get; set; }
}

/// <summary>
/// Folds course events into a catalog. <see cref="GetPublished"/> is the display list.
/// </summary>
public class CourseCatalogProjector
{
    private readonly Dictionary<Guid, CourseCatalogEntry> _courses = [];
    private readonly Lock _lock = new();

    public void Apply(IEvent @event)
    {
        lock (_lock)
        {
            switch (@event)
            {
                case CourseCreated e:
                    _courses[e.CourseId] = new CourseCatalogEntry
                    {
                        CourseId = e.CourseId,
                        Title = e.Title,
                        Description = e.Description,
                        Price = e.Price,
                        Status = CourseCatalogStatus.Draft,
                    };
                    break;

                case CoursePublished e:
                    if (_courses.TryGetValue(e.CourseId, out var published))
                        published.Status = CourseCatalogStatus.Published;
                    break;

                case CoursePriceUpdated e:
                    if (_courses.TryGetValue(e.CourseId, out var priced))
                        priced.Price = e.NewPrice;
                    break;
            }
        }
    }

    public IReadOnlyList<CourseCatalogEntry> GetPublished()
    {
        lock (_lock)
            return [.. _courses.Values.Where(c => c.Status == CourseCatalogStatus.Published)];
    }

    public IReadOnlyList<CourseCatalogEntry> GetAll()
    {
        lock (_lock)
            return [.. _courses.Values];
    }
}
