using MemoryPack;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Patterns.Reaction;
using LawnDart.Demo.Academy.Domain.Student.Events;
using LawnDart.Demo.Academy.Domain.Student.Commands;
using LawnDart.Serialization;

namespace LawnDart.Demo.Academy.EDA;

// ──────────────────────────────────────────────────────────────────────────────
// EDA messages used by the Academy showcase
// ──────────────────────────────────────────────────────────────────────────────

/// <summary>Domain message: seat reservation succeeded — trigger payment processing.</summary>
[MemoryPackable]
[EventTypeName("SeatReservationConfirmed")]
public partial record SeatReservationConfirmed(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid StudentId,
    [property: MemoryPackOrder(3)] Guid CourseId,
    [property: MemoryPackOrder(4)] Guid SectionId,
    [property: MemoryPackOrder(5)] decimal Amount) : IEvent;

/// <summary>Domain message: payment was authorised — trigger enrollment confirmation.</summary>
[MemoryPackable]
[EventTypeName("PaymentAuthorised")]
public partial record PaymentAuthorised(
    [property: MemoryPackOrder(0)] Guid Id,
    [property: MemoryPackOrder(1)] DateTime Timestamp,
    [property: MemoryPackOrder(2)] Guid StudentId,
    [property: MemoryPackOrder(3)] Guid CourseId,
    [property: MemoryPackOrder(4)] string Reference) : IEvent;

// ──────────────────────────────────────────────────────────────────────────────
// Reactor 1 — SeatReservationConfirmed → ProcessPaymentCommand
// ──────────────────────────────────────────────────────────────────────────────

/// <summary>
/// Reacts to a confirmed seat reservation by issuing a payment command.
/// Broker-pattern: at-least-once delivery, inbox-deduplicated upstream.
/// </summary>
public sealed class PaymentReactor : IReactor<SeatReservationConfirmed>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        SeatReservationConfirmed @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        IEnumerable<ICommand> commands =
        [
            new ProcessPaymentCommand(Guid.NewGuid(), @event.StudentId, @event.CourseId, @event.Amount)
        ];
        return Task.FromResult(commands);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Reactor 2 — PaymentAuthorised → SendConfirmationCommand
// ──────────────────────────────────────────────────────────────────────────────

/// <summary>
/// Reacts to a payment authorisation by dispatching an enrollment confirmation command.
/// </summary>
public sealed class ConfirmationReactor : IReactor<PaymentAuthorised>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        PaymentAuthorised @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        IEnumerable<ICommand> commands =
        [
            new SendConfirmationCommand(Guid.NewGuid(), @event.StudentId, @event.CourseId,
                $"student-{@event.StudentId:N}@academy.example")
        ];
        return Task.FromResult(commands);
    }
}
