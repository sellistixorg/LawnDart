using LawnDart;

namespace LawnDart.Demo.Academy.Domain.CourseSection.Commands;

public record CreateSectionCommand(
    Guid Id,
    Guid SectionId,
    Guid CourseId,
    string Title,
    int TotalSeats) : ICommand;

public record ReserveSeatCommand(
    Guid Id,
    Guid SectionId,
    Guid StudentId) : ICommand;

public record ReleaseSeatCommand(
    Guid Id,
    Guid SectionId,
    Guid StudentId) : ICommand;
