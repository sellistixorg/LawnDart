using LawnDart;

namespace LawnDart.Demo.Academy.Domain.Course.Commands;

public record CreateCourseCommand(
    Guid Id,
    Guid CourseId,
    string Title,
    string Description,
    decimal Price) : ICommand;

public record PublishCourseCommand(
    Guid Id,
    Guid CourseId) : ICommand;

public record UpdateCoursePriceCommand(
    Guid Id,
    Guid CourseId,
    decimal NewPrice) : ICommand;
