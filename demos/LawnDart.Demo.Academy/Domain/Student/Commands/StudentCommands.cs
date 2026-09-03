using LawnDart;

namespace LawnDart.Demo.Academy.Domain.Student.Commands;

public record RegisterStudentCommand(
    Guid Id,
    Guid StudentId,
    string Name,
    string Email) : ICommand;

public record ProcessPaymentCommand(
    Guid Id,
    Guid StudentId,
    Guid CourseId,
    decimal Amount) : ICommand;

public record EnrollStudentCommand(
    Guid Id,
    Guid StudentId,
    Guid CourseId) : ICommand;

public record SendConfirmationCommand(
    Guid Id,
    Guid StudentId,
    Guid CourseId,
    string Email) : ICommand;

public record CancelRegistrationCommand(
    Guid Id,
    Guid StudentId,
    Guid CourseId,
    string Reason) : ICommand;
