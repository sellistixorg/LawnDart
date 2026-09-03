using MemoryPack;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.Academy.Domain.CourseSection.Events;

[MemoryPackable]
[EventTypeName("SectionCreated")]
public partial record SectionCreated(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid SectionId,
    [property: MemoryPackOrder(3)] Guid CourseId,
    [property: MemoryPackOrder(4)] string Title,
    [property: MemoryPackOrder(5)] int TotalSeats) : IEvent;

[MemoryPackable]
[EventTypeName("SeatReserved")]
public partial record SeatReserved(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid SectionId,
    [property: MemoryPackOrder(3)] Guid StudentId) : IEvent;

[MemoryPackable]
[EventTypeName("SeatReleased")]
public partial record SeatReleased(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid SectionId,
    [property: MemoryPackOrder(3)] Guid StudentId) : IEvent;
