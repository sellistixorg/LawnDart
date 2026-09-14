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
- Types: `BddTestContext`, `AggregateSpec`, `DcbSpec`, `BddProjectionRunner`,
  `BddSpecAssertionException`

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

## Exception contract

Spec-internal failures — missing `When`, a `Then*` mismatch, an unexpected
exception from `When`, or a broken store probe — throw
`BddSpecAssertionException`. That type does **not** derive from
`InvalidOperationException`.

Do not wrap `RunAsync()` in
`Assert.ThrowsAsync<InvalidOperationException>`. A failed spec assertion
would have passed that test. Use `ThenThrows<T>()` for a domain rule:

- `ThenThrows<T>()` passes when the domain throws `T` or a type derived from `T`.
- A mismatch throws `BddSpecAssertionException` and names both the expected
  and actual types.
- Domain code may still throw `InvalidOperationException` (or
  `DomainException`, which derives from it). Those remain the When
  exception; only the spec runner uses `BddSpecAssertionException`.

A store that does not implement `GetCurrentSequenceAsync` or
`ReadByQueryAsync` throws `NotSupportedException`. The spec treats that as
a missing capability: sequence baseline becomes `-1`, and the appended-stream
set is empty. Any other exception from those probes is a broken store and
surfaces as `BddSpecAssertionException`.
