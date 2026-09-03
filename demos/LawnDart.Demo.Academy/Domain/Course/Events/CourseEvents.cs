using MemoryPack;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;

namespace LawnDart.Demo.Academy.Domain.Course.Events;

[MemoryPackable]
[EventTypeName("AcademyCourseCreated")]
public partial record CourseCreated(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid CourseId,
    [property: MemoryPackOrder(3)] string Title,
    [property: MemoryPackOrder(4)] string Description,
    [property: MemoryPackOrder(5)] decimal Price) : IEvent;

[MemoryPackable]
[EventTypeName("AcademyCoursePublished")]
public partial record CoursePublished(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid CourseId) : IEvent;

[MemoryPackable]
[EventTypeName("AcademyCoursePriceUpdated")]
public partial record CoursePriceUpdated(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid CourseId,
    [property: MemoryPackOrder(3)] decimal NewPrice) : IEvent;
