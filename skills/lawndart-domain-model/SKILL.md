---
name: lawndart-domain-model
description: Author LawnDart commands, events, aggregates, DCB entities, tags, and stream IDs. Keep IEvent Id and Timestamp first.
---

# Domain model

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs).
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization. Do not write a `HandleAsync<TCommand>` switch on the entity (obsolete).
3. **Load** with `GetOrCreateAsync<T>(id)` when the stream is `{type}:{guid}`; use `GetOrCreateAsync<T>(streamId)` otherwise.
4. **Projections:** `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** `UseInMemory` / `UseSqlServer` on `AddBoundedContext(name)`.

```csharp
public sealed record IncrementCommand(Guid Id, Guid CounterId) : ICommand;

public sealed record CounterIncremented(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;

public sealed class Counter : AggregateRoot<CounterState>
{
    public void Handle(IncrementCommand command) { /* Apply events */ }
    protected override void ApplyEventToState(IEvent @event) { }
}
```

Stream IDs: `{tenant}:{type}:{id}`. Tags for DCB: `type:{id}`.
Load with `GetOrCreateAsync<T>(id)` when that shape holds; use
`GetOrCreateAsync<T>(streamId)` for custom IDs. Do not call `SetStreamId`
or `ReplayEvents` in app code.

DCB when a rule spans identities — `DcbEntity` + `IDcbRepository`, not two
aggregates plus a distributed transaction. Aggregates `Apply` events; DCB
entities `Emit` with tags. Author `Handle(TCommand)`; persist with
`HandleCommandAsync`. Broker reactions are `IReactor`; in-process DCB
reactions are `IDcbReactor`. These names stay. See
`docs/GLOSSARY.md` (Intentional verb differences).

Read models are not part of the aggregate. Use Lightweight
`ProjectionBase<TView>` (see `lawndart-projection-authoring`) or your own
projector. Do not implement `IProjector` unless you are writing your own
fold (`IProjector` is experimental). Multi-stream Lightweight views
implement `IMultiStreamEntityResolver`.

See `docs/STREAM_IDS.md`, `docs/TAGGING.md`, `docs/DCB_PATTERNS.md`.
