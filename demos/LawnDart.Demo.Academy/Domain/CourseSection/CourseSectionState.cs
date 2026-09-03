using LawnDart;

namespace LawnDart.Demo.Academy.Domain.CourseSection;

public class CourseSectionState : IState
{
    public Guid SectionId { get; set; }
    public Guid CourseId { get; set; }
    public string Title { get; set; } = string.Empty;
    public int TotalSeats { get; set; }
    public int SeatsReserved { get; set; }

    public int SeatsAvailable => TotalSeats - SeatsReserved;
}
