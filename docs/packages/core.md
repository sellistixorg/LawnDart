# LawnDart

Contracts and host core. Package id is `LawnDart`, not `LawnDart.Core`.
Every other package depends on this one.

Take it first. It does not include an event-store implementation.

## What you get

- `ICommand`, `IEvent`, `IState` — no empty `IMessage` marker
- `[EventTypeName]` catalog tokens and `WithEventTypes` / `EventTypeNameResolver`
- `EventMetadata` / `CommandMetadata` (`TraceId`, `SpanId`, correlation, causation)
- `ICommandDispatcher` — routes reactor/processor commands to `ICommandHandler<T>` (same `LawnDart.Messaging` namespace as `MessageContext`)
- `AggregateRoot<TState>`, `DcbEntity<TState>`, and their repository interfaces
- `IEventStore` — the store **abstraction** (implementations are in other packages)
- Authorization attributes and `AuthorizationService`
- `AddLawnDart`, `AddBoundedContext`, `AddLawnDartAuthorization`
- Hosted pattern contracts (`IReactor`, `IEventProcessor`, `ITaskProcessor`) and
  planned `[Experimental]` stubs (`ICommandDelegator`, `IDownstreamActivity`,
  `IEventGenerator`, `IStateTransformer`) — see the matrix below. Implementing
  a 🔧 type does not register or run it.
- `IProjector` — experimental unused stub; neither host calls it. Not
  the authoring API. Author `ProjectionBase<TView>` plus scope attributes
  for Lightweight. Multi-stream views (including Flywheel) implement
  `IMultiStreamEntityResolver` on that handler.

| From \ To | **Command** | **Event** | **State** |
|---|---|---|---|
| **Command** | Delegation 🔧 | Aggregate Root & DCB ✅ | Downstream Activity 🔧 |
| **Event** | Reaction ✅ | Event Processing ✅ | Projection ✅ |
| **State** | Task Processing ✅ | Event Generator 🔧 | State Transformation 🔧 |

✅ Hosted (runtime, DI, tests) · 🔧 Planned interface — `[Experimental]`, no host yet. Implementing a 🔧 type does not register or run it.

## Registration

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .WithEventTypes(typeof(SomeEvent).Assembly);
```

`AddBoundedContext` only starts the named context. Pair it with
`UseInMemory()` or `UseSqlServer(...)` from an EventSourcing package.
`WithEventTypes` registers the event catalog (required `[EventTypeName]`,
unique tokens). In-memory GWT may skip it; writes still need the attribute.

## Portable store contract

`IEventStore` inherits `IStreamRegistry`. Do not split them. Store
implementers (InMemory, SQL Server, or a third-party backend) must keep
these methods and signatures. `IEventStoreSubscriptions` is the portable
push surface; implement it when the store can deliver a continuous log.

**`IEventStore`**

- `ReadStreamAsync(string streamId, long fromVersion = 0, long? toVersion = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)` — `toTimestamp` is envelope business time (`EventMetadata.Timestamp`), not `CommitTimestamp`.
- `ReadStreamEnumerableAsync(string streamId, long fromVersion = 0, long? toVersion = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)`
- `ReadByQueryAsync(Query query, long? fromSequencePosition = null, int? limit = null, long? toSequencePosition = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)`
- `ReadByQueryStreamAsync(Query query, long? fromSequencePosition = null, long? toSequencePosition = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)`
- `AppendAsync(string streamId, IEnumerable<IEvent> events, long? expectedVersion = null, EventMetadata? metadata = null, IEnumerable<string>? tags = null, CancellationToken cancellationToken = default)`
- `AppendAsync(IEnumerable<IEvent> events, AppendCondition condition, EventMetadata? metadata = null, IEnumerable<string>? tags = null, CancellationToken cancellationToken = default)`
- `GetCurrentSequenceAsync(CancellationToken cancellationToken = default)`
- `GetMaxSequencePositionAsync(Query query, long? fromSequencePosition = null, long? toSequencePosition = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)`

**`IStreamRegistry`** (on every `IEventStore`)

- `GetStreamAsync(string streamId, CancellationToken cancellationToken = default)`
- `GetStreamsByAggregateTypeAsync(string aggregateType, CancellationToken cancellationToken = default)`
- `GetStreamsByTagAsync(string tag, CancellationToken cancellationToken = default)`
- `EnumerateStreamIdsAsync(string? prefix = null, CancellationToken cancellationToken = default)`
- `GetStreamsUpdatedAfterAsync(long afterSequencePosition, int? limit = null, CancellationToken cancellationToken = default)`
- `GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)`

**`IEventStoreSubscriptions`**

- `Subscribe(string subscriberId, long fromSequence, EventSubscriptionFilter? filter = null, CancellationToken cancellationToken = default)`

The Core package ships `PublicAPI.Shipped.txt`. Adding or removing a public
member fails the build (`RS0016` / `RS0017`) unless that file is updated.
See [Backend selection](../BACKEND_SELECTION.md).

## Related

- [Package map](README.md)
- [DI Grammar](../DI_GRAMMAR.md)
- [Overview](../OVERVIEW.md)
