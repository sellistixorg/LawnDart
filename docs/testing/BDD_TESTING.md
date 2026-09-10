# BDD testing

`LawnDart.Testing` runs Given / When / Then specs against `UseInMemory()`.

Events used in `Given` / `When` must declare `[EventTypeName]`. In-memory GWT
does not call `WithEventTypes`; the attribute is enough for writes. After
`RunAsync`, `result.EmittedSequencedEvents` carries envelope `CausationId`
(the command id when the caller left it unset) and a stable `CorrelationId`.
Those fields live on the envelope, not on `ICommand` / `IEvent`.

## Package

- `LawnDart.Testing`
- Namespace `LawnDart.Testing.Bdd`
- Types: `BddTestContext`, `AggregateSpec`, `DcbSpec`, `BddProjectionRunner`

## Aggregate spec

```csharp
await using var ctx = BddTestContext.CreateInMemory();
var id = Guid.NewGuid();

var result = await AggregateSpec
    .For<Counter>(ctx, id)
    .Given(new CounterCreated(Guid.NewGuid(), DateTime.UtcNow, id))
    .When(new IncrementCommand(Guid.NewGuid(), id))
    .ThenEmittedEvent<CounterIncremented>(e => e.CounterId == id)
    .AndExpectedVersion(2)
    .RunAsync();
```

## DCB spec

```csharp
await using var ctx = BddTestContext.CreateInMemory();
var tags = new[] { $"counter:{id:N}" };

await DcbSpec
    .For<CounterEntity, IncrementCommand>(ctx, tags, new IncrementCommand(Guid.NewGuid(), id))
    .Given(new CounterCreated(Guid.NewGuid(), DateTime.UtcNow, id), tags)
    .ThenEmittedEvent<CounterIncremented>(e => e.CounterId == id)
    .RunAsync();
```

## Test project

Reference `LawnDart`, `LawnDart.EventSourcing`, `LawnDart.Testing`, plus your
domain project. Use xUnit. Canonical proofs:
`tests/LawnDart.Testing.Tests/Bdd/InMemoryGwtTests.cs`.

These specs match the Eventhesis GWT widget — see [EVENTHESIS.md](../EVENTHESIS.md).
