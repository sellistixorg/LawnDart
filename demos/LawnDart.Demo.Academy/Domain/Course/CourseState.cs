using LawnDart;

namespace LawnDart.Demo.Academy.Domain.Course;

public class CourseState : IState
{
    public Guid CourseId { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public bool IsPublished { get; set; }
}
