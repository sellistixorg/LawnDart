---
name: lawndart-domain-model
description: Author LawnDart commands, events, aggregates, DCB entities, tags, and stream IDs. Keep IEvent Id and Timestamp first.
---

# Domain model

```csharp
public sealed record IncrementCommand(Guid Id, Guid CounterId) : ICommand;

public sealed record CounterIncremented(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;

public sealed class Counter : AggregateRoot<CounterState>
{
    public override Task HandleAsync<TCommand>(TCommand command) { /* Apply events */ return Task.CompletedTask; }
    protected override void ApplyEventToState(IEvent @event) { }
}
```

Stream IDs: `{tenant}:{type}:{id}`. Tags for DCB: `type:{id}`.

DCB when a rule spans identities — `DcbEntity` + `IDcbRepository`, not two
aggregates plus a distributed transaction.

See `docs/STREAM_IDS.md`, `docs/TAGGING.md`, `docs/DCB_PATTERNS.md`.
