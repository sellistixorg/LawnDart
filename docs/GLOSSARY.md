# Glossary

Terms used across LawnDart docs. Avoid inventing a second vocabulary.

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
Command → Command. Re-dispatch without emitting an event first.

**Downstream Activity**  
Command → State. A command updates a read model or external system without
appending an event.

## E

**Event**  
Immutable fact. `IEvent` with `Id` and `Timestamp` first.

**Event Generator**  
State → Event. Emit facts from current state (rare; usually a processor).

**Event Processing**  
Event → Event. Transform or enrich events without a command.

**Eventhesis**  
Canvas that compiles widgets to LawnDart types. See [EVENTHESIS.md](EVENTHESIS.md).

## I

**InMemory**  
Process-local `IEventStore`. Zero infrastructure. Default for Academy and tests.

## L

**Lightweight projections**  
In-process projection host (`LawnDart.Projections.Lightweight`).

## N

**Nine common patterns**  
The command–event–state matrix.

## O

**Outbox**  
Persist outgoing messages with the same commit as events (SQL Server), then
publish to a transport.

## P

**Projection**  
Event → State. Incremental read model.

## R

**Reaction**  
Event → Command. `IReactor<TEvent>`.

## S

**SQL Server**  
Durable event store and projection store.

**State**  
Present view: aggregate state, DCB state, or a projection read model.

**Stream ID**  
Identity of an event stream. See [STREAM_IDS.md](STREAM_IDS.md).

## T

**Tag**  
Index key on an event used by DCB queries. See [TAGGING.md](TAGGING.md).

**Task Processing**  
State → Command. `ITaskProcessor` polls read models and emits commands.

**Tenant context**  
Optional `ITenantContextProvider` for metadata on events and stream IDs.
