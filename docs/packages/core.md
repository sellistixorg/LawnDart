# LawnDart

Contracts and host core. Package id is `LawnDart`, not `LawnDart.Core`.
Every other package depends on this one.

Take it first. It does not include an event-store implementation.

## What you get

- `ICommand`, `IEvent`, `IState`
- `[EventTypeName]` family tokens, `WithEventTypes` / `EventTypeCatalog.Materialize`, and `WithUpcasters` / `IEventUpcaster<TTo, TFrom>`. See [Event schema versioning](../EVENT_SCHEMA_VERSIONING.md).
- `EventMetadata` / `CommandMetadata` (`TraceId`, `SpanId`, correlation, causation)
- `ICommandDispatcher` — routes reactor/processor commands to `ICommandHandler<T>` (same `LawnDart.Messaging` namespace as `MessageContext`)
- `AggregateRoot<TState>`, `DcbEntity<TState>`, and their repository interfaces
- `IEventStore` — the typed session **abstraction** (`IEvent` in, `SequencedEvent` out)
- `IEventLog` — the durable log (`AppendEvent` in, `RecordedEvent` out). Third-party stores implement this. `IEventSerializer` is `ReadOnlyMemory<byte>`.
- Authorization attributes and `AuthorizationService`
- `AddLawnDart`, `AddBoundedContext`, `AddLawnDartAuthorization`
- Hosted pattern contracts (`IReactor`, `IEventProcessor`, `ITaskProcessor`).
  Four CES cells are roadmap only — no public type yet.
- `IProjector` — experimental unused stub; neither host calls it. Not
  the authoring API. Author `ProjectionBase<TView>` plus scope attributes
  for Lightweight. Multi-stream views (including Flywheel) implement
  `IMultiStreamEntityResolver` on that handler.

The shapes an event model compiles into — five hosted today — are on the
[CES matrix](../CES_MATRIX.md).

## Registration

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .WithEventTypes(typeof(SomeEvent).Assembly);
```

`AddBoundedContext` only starts the named context. Pair it with
`UseInMemory()` or `UseSqlServer(...)` from an EventSourcing package.
`WithEventTypes` is required. It materializes a scoped catalog (required
`[EventTypeName]`; one-arg is version 1 and implicitly current when it is
the only type for that family). A host that skips it fails at startup.
In-memory GWT passes the same types to `BddTestContext.CreateInMemory`.

## Portable store contract

A third-party backend implements `IEventLog`. LawnDart ships the typed
`IEventStore` adapter over that log. `IEventStore` inherits `IStreamRegistry`.
Do not split them. `IEventStoreSubscriptions` is the portable push surface
for the session; the log has `IEventLogSubscriptions`.

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
- [Analyzers](analyzers.md) — optional `LDT*` schema-versioning diagnostics
- [DI Grammar](../DI_GRAMMAR.md)
- [Overview](../OVERVIEW.md)
