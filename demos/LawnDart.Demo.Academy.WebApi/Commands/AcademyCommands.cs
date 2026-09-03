using LawnDart;
using LawnDart.Authorization;
using LawnDart.Demo.Academy.WebApi.Authorization;

namespace LawnDart.Demo.Academy.WebApi.Commands;

/// <summary>Registers a new student in the system.</summary>
[RequiresPermission(AcademyPermissions.StudentEnroll)]
public record RegisterStudentCommand(
    Guid Id,
    Guid StudentId,
    string Name,
    string Email) : ICommand;

/// <summary>Enrols an existing student in a course section.</summary>
[RequiresPermission(AcademyPermissions.StudentEnroll)]
public record EnrollStudentCommand(
    Guid Id,
    Guid StudentId,
    Guid SectionId) : ICommand;

/// <summary>Cancels an existing enrollment.</summary>
[RequiresPermission(AcademyPermissions.StudentEnroll)]
public record CancelEnrollmentCommand(
    Guid Id,
    Guid StudentId,
    Guid SectionId,
    string Reason) : ICommand;

/// <summary>Creates a new course section with a fixed seat capacity.</summary>
[RequiresPermission(AcademyPermissions.SectionCreate)]
public record CreateCourseSectionCommand(
    Guid Id,
    Guid SectionId,
    string Title,
    int TotalSeats) : ICommand;
