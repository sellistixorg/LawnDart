# LawnDart.Messaging

Reactors, event processors, task processors, and outbox publisher hooks.

```csharp
services.AddMessaging();
services.AddReactor<PaymentReactor, SeatReservationConfirmed>();
services.AddTaskProcessor<OverdueRegistrationProcessor>();
```
