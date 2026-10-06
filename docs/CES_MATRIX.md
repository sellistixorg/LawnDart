# Command-event-state shapes

These are the shapes an event model compiles into. Five are hosted today
(runtime, DI, tests). Four are planned for a later release. They have no
public type until a host exists.

| From \ To | **Command** | **Event** | **State** |
|---|---|---|---|
| **Command** | Delegation 🔧 | Aggregate Root & DCB ✅ | Downstream Activity 🔧 |
| **Event** | Reaction ✅ | Event Processing ✅ | Projection ✅ |
| **State** | Task Processing ✅ | Event Generator 🔧 | State Transformation 🔧 |

✅ Hosted (runtime, DI, tests). 🔧 Planned for a later release (no public type).

Most apps start with aggregate or DCB plus a projection (both hosted).

## Hosted cells

| Shape | From → To | Start here |
|---|---|---|
| Aggregate Root & DCB | Command → Event | [Step 2. First aggregate](learning-path/02-first-aggregate.md). [Step 5. DCB](learning-path/05-dcb-patterns.md) |
| Projection | Event → State | [Step 4. Reading state](learning-path/04-reading-state.md) |
| Reaction | Event → Command | [Step 6. Reactions](learning-path/06-reactions.md) (`IReactor`) |
| Event Processing | Event → Event | [Step 6](learning-path/06-reactions.md) (`IEventProcessor`) |
| Task Processing | State → Command | [Step 6](learning-path/06-reactions.md) (`ITaskProcessor`) |

## Planned cells

Delegation (Command to Command), Downstream Activity (Command to State),
Event Generator (State to Event), and State Transformation (State to State)
are planned for a later release. They stay out of scope until LawnDart hosts
them. Until then LawnDart ships no public type for them, and a class named
after one is not discovered or run. Build on the five hosted shapes above.
