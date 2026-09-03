# EDA patterns

Event-driven choreography in LawnDart uses `LawnDart.Messaging` plus
`LawnDart.Messaging.InMemory` for zero-infra hops.

## Reaction (Event → Command)

```csharp
public sealed class PaymentReactor : IReactor<SeatReservationConfirmed>
{
    public Task<IReadOnlyList<ICommand>> ReactAsync(
        SeatReservationConfirmed @event,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<ICommand>>(
            [new ProcessPaymentCommand(...)]);
}

services.AddReactor<PaymentReactor, SeatReservationConfirmed>();
```

Academy Showcase A wires `InMemoryMessageTransport` so the reservation event
becomes a payment command, then a confirmation command.

## Event processing (Event → Event)

`IEventProcessor<TEvent>` consumes an event and may emit further events or
side work without going through a command.

## Task processing (State → Command)

`ITaskProcessor` polls a read model and emits commands when a condition is
true (overdue registration, SLA timeout). Academy menu item 5 demonstrates
this without a broker.

## Outbox

For SQL Server, persist outgoing messages with the same commit as events.
See [OUTBOX_PATTERN.md](OUTBOX_PATTERN.md). InMemory publishes in-process;
there is no durable outbox relay.
