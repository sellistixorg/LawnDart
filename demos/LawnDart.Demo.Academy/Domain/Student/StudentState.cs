using LawnDart;

namespace LawnDart.Demo.Academy.Domain.Student;

public class StudentState : IState
{
    public Guid StudentId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public bool IsActive { get; set; }
    public List<Guid> EnrolledCourseIds { get; set; } = [];
}
