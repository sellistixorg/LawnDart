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

## B

**Bounded context**  
A named DI slice (`AddBoundedContext("orders")`) with its own keyed event store
and optional projections. Not the same as multi-tenancy.

## C

**Checkpoint**  
Last processed event position for a projector. InMemory or SQL Server.

**Command**  
Intent to change the future. `ICommand` with a `Guid Id` idempotency key.
Commands can be rejected; events cannot.

**ConcurrencyException**  
Expected stream version or DCB `AppendCondition` was not satisfied.

## D

**DCB (Dynamic Consistency Boundary)**  
Load events by tags, enforce multi-entity rules, append atomically.
See [DCB_PATTERNS.md](DCB_PATTERNS.md).

**DcbEntity**  
Base type for tag-based entities (`DcbEntity` / `DcbEntity<TState>`).

**Delegation**  
Command → Command. `[Experimental]` planned interface (`ICommandDelegator`); no host yet. Implementing it does not register or run it.

**Downstream Activity**  
Command → State. `[Experimental]` planned interface (`IDownstreamActivity`); no host yet. Implementing it does not register or run it.

## E

**Event**  
Immutable fact. `IEvent` with `Id` and `Timestamp` first.

**Event clocks**  
Three times on the envelope, do not mix them:
- **Business time** — `IEvent.Timestamp` and `EventMetadata.Timestamp` (same after enrich). `toTimestamp` / time-travel uses this.
- **Commit time** — `EventMetadata.CommitTimestamp`, set only at `AppendAsync`. Lag and ops, not domain queries.
- **Trace** — `TraceId` / `SpanId` (W3C hex). The Activity clock is not stored as a third `DateTime`.

**Event Generator**  
State → Event. `[Experimental]` planned interface (`IEventGenerator`); no host yet. Implementing it does not register or run it.

**Event Processing**  
Event → Event. Transform or enrich events without a command.

**Eventhesis**  
Separate event-modelling canvas that emits slice-based JSON. It does not
generate LawnDart types. See [EVENTHESIS.md](EVENTHESIS.md).

## I

**InMemory**  
Process-local `IEventStore`. Zero infrastructure. Default for Academy and tests.

## L

**Lightweight projections**  
In-process projection host (`LawnDart.Projections.Lightweight`).

## N

**Nine common patterns**  
The command–event–state matrix. Five cells are hosted; four are planned
interfaces with no host. See the root [README](../README.md#pattern-matrix).

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

## S

**SQL Server**  
Durable event store and projection store.

**State**  
Present view: aggregate state, DCB state, or a projection read model.

**State Transformation**  
State → State. `[Experimental]` planned interface (`IStateTransformer`); no host yet. Implementing it does not register or run it.

**Stream ID**  
Identity of an event stream. See [STREAM_IDS.md](STREAM_IDS.md).

## T

**Tag**  
Index key on an event used by DCB queries. See [TAGGING.md](TAGGING.md).

**Task Processing**  
State → Command. `ITaskProcessor` polls read models and emits commands.

**Tenant context**  
Optional `ITenantContextProvider` for metadata on events and stream IDs.
