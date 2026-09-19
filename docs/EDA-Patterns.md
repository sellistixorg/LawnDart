# EDA patterns

Event-driven choreography uses `LawnDart.Messaging` plus
`LawnDart.Messaging.InMemory` for zero-infra hops.

## Reaction (Event → Command)

Academy `PaymentReactor`:

```csharp
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

services.AddReactor<PaymentReactor, SeatReservationConfirmed>();
```

`IReactor<TEvent>` is the broker path (`MessageContext`). `IDcbReactor`
is the in-process DCB path (`EventMetadata`, `GetTagsForReaction()`).
See [Intentional verb differences](GLOSSARY.md#intentional-verb-differences).

Academy Showcase A wires `InMemoryMessageTransport` so the reservation
event becomes a payment command, then a confirmation command.

## Event processing (Event → Event)

`IEventProcessor<TEvent>` consumes an event and may emit further events
or side work without going through a command.

## Task processing (State → Command)

`ITaskProcessor` polls a read model and emits commands when a condition
is true (overdue registration, SLA timeout). Academy menu item 5
demonstrates this without a broker.

## Outbox

Use SQL Server plus `EnableOutbox` when the consumer is another process.
See [OUTBOX_PATTERN.md](OUTBOX_PATTERN.md). InMemory publishes in-process.
