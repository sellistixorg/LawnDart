# Command–event–state shapes

LawnDart is the .NET runtime that event-modeled systems compile into.

These are the shapes an event model compiles into. Five are hosted today
(runtime, DI, tests). Four are roadmap — no public type and no host.

| From \ To | **Command** | **Event** | **State** |
|---|---|---|---|
| **Command** | Delegation 🔧 | Aggregate Root & DCB ✅ | Downstream Activity 🔧 |
| **Event** | Reaction ✅ | Event Processing ✅ | Projection ✅ |
| **State** | Task Processing ✅ | Event Generator 🔧 | State Transformation 🔧 |

✅ Hosted (runtime, DI, tests) · 🔧 Not hosted (roadmap — no public type).

You do not need all nine on day one. Most apps start with aggregate or DCB
plus a projection (both ✅).

## Hosted cells

| Shape | From → To | Start here |
|---|---|---|
| Aggregate Root & DCB | Command → Event | [Step 2 — First aggregate](learning-path/02-first-aggregate.md) · [Step 5 — DCB](learning-path/05-dcb-patterns.md) |
| Projection | Event → State | [Step 4 — Reading state](learning-path/04-reading-state.md) |
| Reaction | Event → Command | [Step 6 — Reactions](learning-path/06-reactions.md) (`IReactor`) |
| Event Processing | Event → Event | [Step 6](learning-path/06-reactions.md) (`IEventProcessor`) |
| Task Processing | State → Command | [Step 6](learning-path/06-reactions.md) (`ITaskProcessor`) |

## Roadmap cells

Delegation (Command → Command), Downstream Activity (Command → State),
Event Generator (State → Event), and State Transformation (State → State)
are named shapes with no host and no public type. Implementing a type by
those names does not register or run anything. They come back when a host
exists.
