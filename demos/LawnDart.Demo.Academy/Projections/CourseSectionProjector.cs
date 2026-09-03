using LawnDart;
using LawnDart.Demo.Academy.Domain.CourseSection.Events;

namespace LawnDart.Demo.Academy.Projections;

/// <summary>
/// Read model for a single section: live available seat count.
/// Updated by <see cref="CourseSectionProjector"/>.
/// </summary>
public class SectionSeatView
{
    public Guid SectionId { get; set; }
    public Guid CourseId { get; set; }
    public string SectionTitle { get; set; } = string.Empty;
    public int TotalSeats { get; set; }
    public int SeatsReserved { get; set; }
    public int SeatsAvailable => TotalSeats - SeatsReserved;
    public int EnrollmentCount { get; set; }
    public DateTimeOffset LastUpdated { get; set; }
}

/// <summary>
/// Simple in-process projector: projects CourseSection events into <see cref="SectionSeatView"/>.
/// Demonstrates the Projection pattern — a read-side state built from domain events.
/// Used by the Live Projection Panel in Showcase C.
/// </summary>
public class CourseSectionProjector
{
    private readonly Dictionary<Guid, SectionSeatView> _views = [];
    private readonly Lock _lock = new();

    public void Apply(IEvent @event)
    {
        lock (_lock)
        {
            switch (@event)
            {
                case SectionCreated e:
                    _views[e.SectionId] = new SectionSeatView
                    {
                        SectionId = e.SectionId,
                        CourseId = e.CourseId,
                        SectionTitle = e.Title,
                        TotalSeats = e.TotalSeats,
                        SeatsReserved = 0,
                        LastUpdated = DateTimeOffset.UtcNow,
                    };
                    break;

                case SeatReserved e:
                    if (_views.TryGetValue(e.SectionId, out var rv))
                    {
                        rv.SeatsReserved++;
                        rv.LastUpdated = DateTimeOffset.UtcNow;
                    }
                    break;

                case SeatReleased e:
                    if (_views.TryGetValue(e.SectionId, out var rlv))
                    {
                        rlv.SeatsReserved = Math.Max(0, rlv.SeatsReserved - 1);
                        rlv.LastUpdated = DateTimeOffset.UtcNow;
                    }
                    break;

                case Domain.Student.Events.StudentEnrolled e:
                    foreach (var view in _views.Values.Where(v => v.CourseId == e.CourseId))
                    {
                        view.EnrollmentCount++;
                        view.LastUpdated = DateTimeOffset.UtcNow;
                    }
                    break;
            }
        }
    }

    public IReadOnlyList<SectionSeatView> GetAll()
    {
        lock (_lock)
            return [.. _views.Values];
    }

    public SectionSeatView? Get(Guid sectionId)
    {
        lock (_lock)
            return _views.TryGetValue(sectionId, out var v) ? v : null;
    }
}
