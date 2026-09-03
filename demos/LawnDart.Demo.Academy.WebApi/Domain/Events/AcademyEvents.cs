using LawnDart;

namespace LawnDart.Demo.Academy.WebApi.Domain.Events;

/// <summary>Raised when a new student is registered in the system.</summary>
public record StudentRegistered(
    Guid Id,
    DateTime Timestamp,
    Guid StudentId,
    string Name,
    string Email) : IEvent;

/// <summary>Raised when a student enrols in a course section.</summary>
public record StudentEnrolled(
    Guid Id,
    DateTime Timestamp,
    Guid StudentId,
    Guid SectionId,
    string SectionTitle) : IEvent;

/// <summary>Raised when a student's enrollment is cancelled.</summary>
public record EnrollmentCancelled(
    Guid Id,
    DateTime Timestamp,
    Guid StudentId,
    Guid SectionId,
    string Reason) : IEvent;

/// <summary>Raised when a new course section is created.</summary>
public record SectionCreated(
    Guid Id,
    DateTime Timestamp,
    Guid SectionId,
    string Title,
    int TotalSeats) : IEvent;

/// <summary>Raised when a seat in a section is reserved for a student.</summary>
public record SeatReserved(
    Guid Id,
    DateTime Timestamp,
    Guid SectionId,
    Guid StudentId) : IEvent;

/// <summary>Raised when a previously reserved seat is released.</summary>
public record SeatReleased(
    Guid Id,
    DateTime Timestamp,
    Guid SectionId,
    Guid StudentId) : IEvent;
