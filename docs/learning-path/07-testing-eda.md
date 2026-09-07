# Step 7 — Testing EDA

**Previous:** [Reactions](06-reactions.md) · **Next:** [Production](08-production.md)

`LawnDart.Testing` messaging harnesses stay on InMemory transports. Drive the
`PaymentReactor` from [step 6](06-reactions.md) in isolation with
`ReactorTestHarness<TReactor, TEvent>` — no host, no broker.

```csharp
using LawnDart.Messaging;
using LawnDart.Testing;

var harness = new ReactorTestHarness<PaymentReactor, SeatReservationConfirmed>(
    new PaymentReactor());

var evt = new SeatReservationConfirmed(Guid.NewGuid(), DateTime.UtcNow, studentId, Amount: 299m);
var context = MessageContext.New();

var first = await harness.ReactAsync(evt, context);
Assert.Single(first);
Assert.IsType<ProcessPaymentCommand>(first[0]);

var replay = await harness.ReactAsync(evt, context);
Assert.Empty(replay); // same MessageId — inbox deduplicated
```

Assert:

- The reactor emitted the expected command type.
- Inbox / idempotency: the same `MessageId` does not enroll twice.
- Task processors emit only for overdue rows.

When projections or hosted reactors are asynchronous, poll instead of
sleeping:

```csharp
await WaitForAsync.UntilAsync(() => view.Confirmed);
```

SQL Server integration tests are optional and tagged `Category=Integration`.

Full aggregate / DCB given-when-then: [BDD_TESTING.md](../testing/BDD_TESTING.md).
