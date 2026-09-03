using MemoryPack;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.Academy.Domain.Student.Events;

[MemoryPackable]
[EventTypeName("StudentRegistered")]
public partial record StudentRegistered(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid StudentId,
    [property: MemoryPackOrder(3)] string Name,
    [property: MemoryPackOrder(4)] string Email) : IEvent;

[MemoryPackable]
[EventTypeName("StudentPaymentProcessed")]
public partial record StudentPaymentProcessed(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid StudentId,
    [property: MemoryPackOrder(3)] Guid CourseId,
    [property: MemoryPackOrder(4)] decimal Amount,
    [property: MemoryPackOrder(5)] string Reference) : IEvent;

[MemoryPackable]
[EventTypeName("StudentEnrolled")]
public partial record StudentEnrolled(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid StudentId,
    [property: MemoryPackOrder(3)] Guid CourseId) : IEvent;

[MemoryPackable]
[EventTypeName("RegistrationConfirmationSent")]
public partial record RegistrationConfirmationSent(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid StudentId,
    [property: MemoryPackOrder(3)] Guid CourseId,
    [property: MemoryPackOrder(4)] string Email) : IEvent;

[MemoryPackable]
[EventTypeName("RegistrationCancelled")]
public partial record RegistrationCancelled(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid StudentId,
    [property: MemoryPackOrder(3)] Guid CourseId,
    [property: MemoryPackOrder(4)] string Reason) : IEvent;
