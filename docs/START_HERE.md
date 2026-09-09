# Start Here

LawnDart is the in-process .NET runtime for commands, events, and state.
[Eventhesis](https://eventhesis.com) is an optional modelling canvas that emits
slice-based event-model JSON. It does not generate LawnDart types.

## Core mental model

- **Command**: intent to change something in the future.
- **Event**: immutable fact about what already happened.
- **State**: current read model used by users and workflows.

Most applications start with the **hosted** cells (✅): aggregate or DCB,
projection, then optional reactions or task processors. Four cells (🔧) are
planned interfaces only — no host yet.

| From \ To | **Command** | **Event** | **State** |
|---|---|---|---|
| **Command** | Delegation 🔧 | Aggregate Root & DCB ✅ | Downstream Activity 🔧 |
| **Event** | Reaction ✅ | Event Processing ✅ | Projection ✅ |
| **State** | Task Processing ✅ | Event Generator 🔧 | State Transformation 🔧 |

✅ Hosted (runtime, DI, tests) · 🔧 Planned interface — `[Experimental]`, no host yet. Implementing a 🔧 type does not register or run it.

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

1. [Overview](OVERVIEW.md)
2. [Quickstart](QUICKSTART.md) (InMemory)
3. [Learning Path](learning-path/README.md)
4. [Eventhesis and LawnDart](EVENTHESIS.md)
