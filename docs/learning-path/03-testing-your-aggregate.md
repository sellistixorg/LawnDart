# Step 3 — Testing your aggregate

**Previous:** [First aggregate](02-first-aggregate.md) · **Next:** [Reading state](04-reading-state.md)

Use `LawnDart.Testing` against InMemory. No Docker. Events from
[step 2](02-first-aggregate.md) already declare `[EventTypeName]`; GWT writes
need the attribute even though `CreateInMemory()` does not call
`WithEventTypes`.

```csharp
await using var ctx = BddTestContext.CreateInMemory();

await AggregateSpec
    .For<Counter>(ctx, id)
    .Given(new CounterCreated(Guid.NewGuid(), DateTime.UtcNow, id))
    .When(new IncrementCommand(Guid.NewGuid(), id))
    .ThenEmittedEvent<CounterIncremented>(e => e.CounterId == id)
    .RunAsync();
```

Full guide: [BDD_TESTING.md](../testing/BDD_TESTING.md).

SQL Server tests are tagged `Category=Integration` and need Docker. Local
unit runs use `--filter Category!=Integration`. CI runs both.
