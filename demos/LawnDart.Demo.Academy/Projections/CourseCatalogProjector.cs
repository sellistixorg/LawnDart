using LawnDart;
using LawnDart.Demo.Academy.Domain.Course.Events;

namespace LawnDart.Demo.Academy.Projections;

/// <summary>
/// Lifecycle status of a course in the catalog read model.
/// </summary>
public enum CourseStatus
{
    /// <summary>Course is being authored and is not listed.</summary>
    Draft,

    /// <summary>Course is live and included in the published catalog.</summary>
    Published,

    /// <summary>Course is no longer available for new enrollments.</summary>
    Archived
}

/// <summary>
/// One course row in the catalog: identity, copy, current price, and status.
/// Updated by <see cref="CourseCatalogProjector"/>.
/// </summary>
public class CourseCatalogView
{
    /// <summary>Gets or sets the course identifier.</summary>
    public Guid CourseId { get; set; }

    /// <summary>Gets or sets the course title.</summary>
    public string Title { get; set; } = string.Empty;

    /// <summary>Gets or sets the course description.</summary>
    public string Description { get; set; } = string.Empty;

    /// <summary>Gets or sets the current enrollment price.</summary>
    public decimal Price { get; set; }

    /// <summary>Gets or sets the current course status.</summary>
    public CourseStatus Status { get; set; }
}

/// <summary>
/// Folds course events into a catalog of published courses and their current price.
/// Draft rows are kept so a later publish still has title, description, and price.
/// </summary>
public class CourseCatalogProjector
{
    private readonly Dictionary<Guid, CourseCatalogView> _views = [];
    private readonly Lock _lock = new();

    /// <summary>
    /// Applies one course event. Other event types are ignored.
    /// </summary>
    public void Apply(IEvent @event)
    {
        lock (_lock)
        {
            switch (@event)
            {
                case CourseCreated e:
                    _views[e.CourseId] = new CourseCatalogView
                    {
                        CourseId = e.CourseId,
                        Title = e.Title,
                        Description = e.Description,
                        Price = e.Price,
                        Status = CourseStatus.Draft,
                    };
                    break;

                case CoursePublished e:
                    if (_views.TryGetValue(e.CourseId, out var published))
                        published.Status = CourseStatus.Published;
                    break;

                case CoursePriceUpdated e:
                    if (_views.TryGetValue(e.CourseId, out var priced))
                        priced.Price = e.NewPrice;
                    break;
            }
        }
    }

    /// <summary>
    /// Returns published courses. Draft and archived rows are omitted.
    /// </summary>
    public IReadOnlyList<CourseCatalogView> GetPublished()
    {
        lock (_lock)
            return [.. _views.Values.Where(v => v.Status == CourseStatus.Published)];
    }

    /// <summary>
    /// Returns the catalog row for one course, including drafts.
    /// </summary>
    public CourseCatalogView? Get(Guid courseId)
    {
        lock (_lock)
            return _views.TryGetValue(courseId, out var view) ? view : null;
    }
}
