# Start Here

LawnDart is the .NET runtime that event-modeled systems compile into.

[Eventhesis](https://eventhesis.com) is one modelling tool that targets
LawnDart. It emits slice-based event-model JSON. It does not generate LawnDart
types. A whiteboard or any canvas is enough. See [Why LawnDart](WHY.md).

## Core mental model

- **Command**: intent to change something in the future.
- **Event**: immutable fact about what already happened.
- **State**: current read model used by users and workflows.

Most applications start with an aggregate or DCB, then a projection, then
optional reactions. The shapes are on the [CES matrix](CES_MATRIX.md).

Typical first path:

1. Command handling through an aggregate or DCB handler.
2. Events appended to an event store.
3. State rebuilt through projections.
4. Optional reactions / event processors for asynchronous workflows.

## What to use first

- **Core contracts**: `LawnDart` (`AddLawnDart`)
- **Event-sourcing runtime**: `LawnDart.EventSourcing`
- **Storage**: `UseInMemory()` (zero infra) or `UseSqlServer(...)` (durable)
- **Projections**: `LawnDart.Projections.Lightweight` via `.WithProjections(...)`
- **HTTP**: `LawnDart.AspNetCore` plus optional `LawnDart.Authorization.AspNetCore`

## Recommended path

1. [Why LawnDart](WHY.md)
2. [Overview](OVERVIEW.md)
3. [Quickstart](QUICKSTART.md) (InMemory)
4. [Learning Path](learning-path/README.md)
5. [CES matrix](CES_MATRIX.md)
6. [Eventhesis adapter](EVENTHESIS.md)
