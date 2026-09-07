# Step 6 — Reactions and EDA

**Previous:** [DCB](05-dcb-patterns.md) · **Next:** [Testing EDA](07-testing-eda.md)

`IReactor<TEvent>` turns an event into commands. Pair it with
`AddInMemoryMessaging()` when you want Academy-style choreography without a
broker.

```
SeatReservationConfirmed → PaymentReactor → ProcessPaymentCommand
PaymentAuthorised        → ConfirmationReactor → SendConfirmationCommand
```

```csharp
using LawnDart;
using LawnDart.Messaging;
using LawnDart.Patterns.Reaction;

public sealed record SeatReservationConfirmed(
    Guid Id, DateTime Timestamp, Guid StudentId, decimal Amount) : IEvent;
public sealed record ProcessPaymentCommand(
    Guid Id, Guid StudentId, decimal Amount) : ICommand;

public sealed class PaymentReactor : IReactor<SeatReservationConfirmed>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        SeatReservationConfirmed @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IEnumerable<ICommand>>(
            [new ProcessPaymentCommand(Guid.NewGuid(), @event.StudentId, @event.Amount)]);
}

services.AddInMemoryMessaging();
services.AddReactor<PaymentReactor, SeatReservationConfirmed>();
```

`AddReactor` registers a hosted service that pulls from `IMessageTransport`.
You can also call the reactor directly to see the command it emits:

```csharp
var commands = await new PaymentReactor().ReactAsync(
    new SeatReservationConfirmed(Guid.NewGuid(), DateTime.UtcNow, studentId, Amount: 299m),
    MessageContext.New());
// commands is [ProcessPaymentCommand]
```

The other two EDA patterns:

| Pattern | Interface | Direction |
|---|---|---|
| Reaction | `IReactor<TEvent>` | Event → Command |
| Event processing | `IEventProcessor<TEvent>` | Event → Event / side work |
| Task processing | `ITaskProcessor` | State → Command (poll a read model) |

`ITaskProcessor.ProcessTasksAsync` is the overdue-registration counterpart —
Academy menu item 5 runs it without a broker.

Guide: [EDA-Patterns.md](../EDA-Patterns.md).
