# Step 6 — Reactions and EDA

**Previous:** [DCB](05-dcb-patterns.md) · **Next:** [Testing EDA](07-testing-eda.md)

`IReactor<TEvent>` turns an event into commands. Pair with
`AddInMemoryMessaging()` for Academy-style choreography.

```
SeatReservationConfirmed → PaymentReactor → ProcessPaymentCommand
PaymentAuthorised        → ConfirmationReactor → SendConfirmationCommand
```

`ITaskProcessor` is the state → command counterpart (overdue registrations).

Guide: [EDA-Patterns.md](../EDA-Patterns.md).
