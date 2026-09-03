# Start Here

LawnDart is the in-process .NET runtime for commands, events, and state.
[Eventhesis](https://eventhesis.com) is the canvas; this library is what it
compiles to.

## Core mental model

- **Command**: intent to change something in the future.
- **Event**: immutable fact about what already happened.
- **State**: current read model used by users and workflows.

Most applications start with:

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

1. [Overview](OVERVIEW.md)
2. [Quickstart](QUICKSTART.md) (InMemory)
3. [Learning Path](learning-path/README.md)
4. [Eventhesis contract](EVENTHESIS.md)
