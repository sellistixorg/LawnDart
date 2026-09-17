# Glossary

Terms used across LawnDart docs. Avoid inventing a second vocabulary.

## Intentional verb differences

Aggregate and DCB paths use different verbs on purpose. These are **not**
renames waiting to happen.

| Concept | Aggregate / broker | DCB | Why they differ |
|---|---|---|---|
| Record a pending event | `Apply(event)` | `Emit(event, tags)` | `Emit` attaches payload tags for the tag query. `Apply` is stream-scoped and has no tags. |
| Fold an event into state | `ApplyEventToState` | `ApplyEventToState` | Same name on both bases. |
| Author command logic | `Handle(TCommand)` | `Handle(TCommand)` | Same authoring. `HandleAsync<TCommand>` is obsolete. |
| Persist and dispatch | `IAggregateRepository.HandleCommandAsync` | `IDcbRepository.HandleCommandAsync` | Repository verb. Not the same as entity `Handle`. |
| App-facing handler | `ICommandHandler<T>.HandleAsync` | same | Handler interface; not renamed. |
| Reaction | `IReactor<TEvent>.ReactAsync(evt, MessageContext, ct)` | `IDcbReactor.ReactAsync(evt, EventMetadata, ct)` | Broker transport vs in-process tag/metadata. `IDcbReactor` is for in-service workflows without a broker. |
| Projection stub | `IProjector.ProjectAsync` (experimental, unused) | `DcbProjector.ProjectEventAsync` | Different hosts. Author `ProjectionBase` for Lightweight. |
| Cancellation parameter | `cancellationToken` almost everywhere | `ct` in Snapshots | Historical. Do not rename. |

## A

**Aggregate Root**  
A class extending `AggregateRoot<TState>` that handles commands, applies events, and
rebuilds state. The Command → Event pattern.

**AppendCondition**  
A write-time fence for DCB appends. If any later event matches the condition's
tags, the append fails with `ConcurrencyException`.

**AuthorizationContext**  
User roles, permission claims, and entitlements used by `AuthorizationService`.

**AppendEvent**  
Caller-supplied log envelope: family token, schema version, content-type,
payload bytes, UTF-8 JSON metadata bytes, tags. No store-assigned sequence,
stream version, or commit timestamp.

## B

**Bounded context**  
A named DI slice (`AddBoundedContext("orders")`) with its own keyed event store
and optional projections. Not the same as multi-tenancy.

## C

**Catalog token**  
Stable kebab-case **family** name stored for an event type (`[EventTypeName]`).
Required on every concrete `IEvent` that is written. The token does not change
when the payload shape versions (`SchemaVersion` on the log frame). CLR
`FullName` is not stored. `GetName` returns the family token, not
`author-registered.v2`.

`WithEventTypes` calls `EventTypeCatalog.Materialize` and registers that
**scoped immutable** catalog on the bounded context. Resolve-on-read is
`(token, SchemaVersion)` → CLR type. One-arg `[EventTypeName("token")]` is
version 1 and implicitly current when it is the only type for that family. A
family with two or more types needs exactly one `current: true`. Duplicate
`(token, version)` or two currents fail at materialize. Older FullName /
simple-name / AssemblyQualifiedName rows still resolve as read aliases.
`EventTypeNameResolver` is a process-wide compatibility wrapper, not the
context catalog. Current CLR type keeps the domain name; historical is
`AuthorRegisteredV1`. Historical versions reach current through
`IEventUpcaster<TTo, TFrom>` registered with `WithUpcasters`. Optional
`LawnDart.Analyzers` reports `LDT001`–`LDT003` for the same rules at
compile time; warmup is the runtime authority. See
[Event schema versioning](EVENT_SCHEMA_VERSIONING.md).

See **Event log** / **Event store**.

**Checkpoint**  
Last processed event position for a projector. InMemory or SQL Server.

**Command**  
Intent to change the future. `ICommand` with a `Guid Id` idempotency key.
Commands can be rejected; events cannot. Correlation, causation, and W3C
trace are not command payload fields.

**ConcurrencyException**  
Expected stream version or DCB `AppendCondition` was not satisfied.

**Correlation / causation**  
Envelope fields. `CausationId` is the immediate parent (defaults to
`command.Id` on `HandleCommandAsync` when unset). `CorrelationId` is the
saga / W3C trace id — not a copy of causation.

## D

**DCB (Dynamic Consistency Boundary)**  
Load events by tags, enforce multi-entity rules, append atomically.
See [DCB_PATTERNS.md](DCB_PATTERNS.md).

**DcbEntity**  
Base type for tag-based entities (`DcbEntity` / `DcbEntity<TState>`).

**Delegation**
Command → Command. Roadmap cell; no public type and no host yet.

**Downstream Activity**
Command → State. Roadmap cell; no public type and no host yet.

## E

**Envelope**  
`EventMetadata` / `CommandMetadata` / `MessageContext` on the typed session.
Source of truth for event id, business time, commit time, correlation,
causation, `TraceId`, `SpanId`, and tenant. `SchemaName` is the catalog token.
The durable log does not carry `EventMetadata`; see **AppendEvent**.

**Event**  
Immutable fact. `IEvent` with `Id` and `Timestamp` first, plus
`[EventTypeName]`.

**Event clocks**  
Three times on the envelope, do not mix them:
- **Business time** — `IEvent.Timestamp` and `EventMetadata.Timestamp` (same after enrich). `IEventStore.toTimestamp` / time-travel uses this.
- **Commit time** — first-class on `RecordedEvent.CommitTimestamp`; mirrored onto `EventMetadata.CommitTimestamp` by the session. Lag and ops, not domain queries. `IEventLog` may filter `toCommitTimestamp`.
- **Trace** — `TraceId` / `SpanId` (W3C hex). The Activity clock is not stored as a third `DateTime`.

**Event log**  
`IEventLog`: schema-dumb durability. Append `AppendEvent`, read `RecordedEvent`.
Family token, `SchemaVersion`, `ContentType`, payload bytes, metadata JSON
bytes, tags, stream id/version, global sequence, commit timestamp. No CLR
event type. Third-party stores implement this. A process that has **not**
registered event CLR types still appends, filters, and copies frames here.
Typed hydrate (`EventSession` / `IEventStore`) fails closed on an unknown
family — it does not invent a stand-in event.

**Event store**  
`IEventStore`: typed application session over a log. Append/read `IEvent` /
`SequencedEvent`. Stream registry and live subscriptions hydrate in the
session. Unchanged for handlers and aggregates.

**Event Generator**
State → Event. Roadmap cell; no public type and no host yet.

**Event Processing**  
Event → Event. Transform or enrich events without a command.

**Eventhesis**  
Separate event-modelling canvas that emits slice-based JSON. It does not
generate LawnDart types. See [EVENTHESIS.md](EVENTHESIS.md).

## I

**IRawEvent**  
`IEvent` that already has a family token and payload bytes. LawnDart's type is
`RawRecordedEvent`. Not a catalog type and not a typed-hydrate fallback. A
process without the CLR type uses **Event log**.

**InMemory**  
Process-local log (`IEventLog`) plus typed session (`IEventStore`). Serializes
on append; read hydrates a new instance. Not an object heap. Zero
infrastructure. Default for Academy and tests.

## L

**Lightweight projections**  
In-process projection host (`LawnDart.Projections.Lightweight`).

## M

**MessageContext**  
Inbound transport envelope (HTTP, messaging). `ContextAwareCommandDispatcher`
publishes it on `AmbientMessageContext` and continues `traceparent` before
`HandleAsync`.

## N

**Nine common patterns**
The command–event–state matrix. Five cells are hosted; four are roadmap
(no public type yet). See the [pattern matrix](OVERVIEW.md).

## O

**Outbox**  
Persist outgoing messages with the same commit as events (SQL Server), then
publish to a transport.

## P

**Projection**  
Event → State. Incremental read model.

## R

**Reaction**  
Event → Command. Broker path: `IReactor<TEvent>` (`MessageContext`). DCB
in-process path: `IDcbReactor` (`EventMetadata`). Not the same contract —
see **Intentional verb differences**.

**RawRecordedEvent**  
LawnDart's `IRawEvent`: a `RecordedEvent` viewed as an `IEvent` so typed
helpers can read the stored family token. Not a hydrate fallback and not a
catalog type. `OpaqueEvent` / `UnknownEvent` are not LawnDart types.

**RecordedEvent**  
`AppendEvent` fields plus store-assigned stream id, stream version, global
sequence, and commit timestamp. Tags as stored.

## S

**SchemaVersion**  
Integer on the log frame (`AppendEvent` / `RecordedEvent` / SQL column).
Identifies which CLR type in a family to deserialize. Not a Chronicle
"generation". The typed session stamps it from the current CLR type on
append. `EventMetadata.SchemaVersion` is a compatibility mirror written
by the session (append and hydrate). The frame is authority. Missing or
zero on old rows treats as `1`. The catalog resolves
`(token, SchemaVersion)`; the token stays the family name.
See [Event schema versioning](EVENT_SCHEMA_VERSIONING.md)
(including [deploy](EVENT_SCHEMA_VERSIONING.md#deploy)).

**SQL Server**  
Durable event store and projection store.

**State**  
Present view: aggregate state, DCB state, or a projection read model.

**State Transformation**
State → State. Roadmap cell; no public type and no host yet.

**Stream ID**  
Identity of an event stream. See [STREAM_IDS.md](STREAM_IDS.md).

## T

**Tag**  
Index key on an event used by DCB queries. See [TAGGING.md](TAGGING.md).

**Task Processing**  
State → Command. `ITaskProcessor` polls read models and emits commands.

**Tenant context**  
Optional `ITenantContextProvider` for metadata on events and stream IDs.
