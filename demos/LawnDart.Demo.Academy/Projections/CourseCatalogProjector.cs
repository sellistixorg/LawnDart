using LawnDart;
using LawnDart.Demo.Academy.Domain.Course.Events;

namespace LawnDart.Demo.Academy.Projections;

/// <summary>
/// Lifecycle of a course on the catalog read model.
/// </summary>
public enum CourseStatus
{
    /// <summary>Course is being authored and is not listed.</summary>
    Draft,

    /// <summary>Course is live and listed for enrollment.</summary>
    Published,

    /// <summary>Course is no longer available for new enrollments.</summary>
    Archived
}

/// <summary>
/// One course row in the catalog: identity, copy, current price, and status.
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
/// Published-course read model. Folds course created, published, and price-updated
/// events into <see cref="CourseCatalogView"/> rows. <see cref="ListPublished"/>
/// returns only courses whose status is <see cref="CourseStatus.Published"/>.
/// </summary>
public class CourseCatalogProjector
{
    private readonly Dictionary<Guid, CourseCatalogView> _views = [];
    private readonly Lock _lock = new();

    /// <summary>
    /// Applies one course event to the catalog.
    /// </summary>
    public void Apply(IEvent @event)
    {
        lock (_lock)
        {
            switch (@event)
            {
                case CourseCreated e:
                    var created = GetOrCreate(e.CourseId);
                    created.Title = e.Title;
                    created.Description = e.Description;
                    created.Price = e.Price;
                    break;

                case CoursePublished e:
                    GetOrCreate(e.CourseId).Status = CourseStatus.Published;
                    break;

                case CoursePriceUpdated e:
                    if (_views.TryGetValue(e.CourseId, out var priced))
                        priced.Price = e.NewPrice;
                    break;
            }
        }
    }

    /// <summary>
    /// Returns the catalog row for <paramref name="courseId"/>, including drafts.
    /// </summary>
    public CourseCatalogView? Get(Guid courseId)
    {
        lock (_lock)
            return _views.TryGetValue(courseId, out var view) ? view : null;
    }

    /// <summary>
    /// Lists published courses with their current price.
    /// </summary>
    public IReadOnlyList<CourseCatalogView> ListPublished()
    {
        lock (_lock)
            return [.. _views.Values.Where(v => v.Status == CourseStatus.Published)];
    }

    private CourseCatalogView GetOrCreate(Guid courseId)
    {
        if (_views.TryGetValue(courseId, out var existing))
            return existing;

        var created = new CourseCatalogView
        {
            CourseId = courseId,
            Status = CourseStatus.Draft
        };
        _views[courseId] = created;
        return created;
    }
}
