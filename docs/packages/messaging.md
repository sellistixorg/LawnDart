# LawnDart.Messaging

Reactors, event processors, task processors, and outbox publisher hooks.

Take it when an event in one slice should become a command (or further work)
in another. This package is the host and the contracts; it does not include
a transport. Pair it with `LawnDart.Messaging.InMemory` for zero-broker
delivery.

## Registration

```csharp
services.AddMessaging();
services.AddReactor<PaymentReactor, SeatReservationConfirmed>();
services.AddTaskProcessor<OverdueRegistrationProcessor>();
```

`AddInMemoryMessaging()` calls `AddMessaging()` for you.

| Method | Pattern |
|---|---|
| `AddReactor<TReactor, TEvent>` | Event → Command |
| `AddEventProcessor<TProcessor, TEvent>` | Event → Event / side work |
| `AddTaskProcessor<TProcessor>` | State → Command |

## Related

- [Package map](README.md)
- [EDA patterns](../EDA-Patterns.md)
- [Reactions](../learning-path/06-reactions.md)
- [InMemory messaging](messaging-inmemory.md)
