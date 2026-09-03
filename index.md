---
_layout: landing
---

# LawnDart

In-process .NET library for event-sourced commands, events, and state.

LawnDart implements the **nine common patterns** — a command–event–state
matrix of transitions between Command (future intent), Event (past fact), and
State (present view).

## Pattern matrix

| From \ To | **Command** | **Event** | **State** |
|-----------|-------------|-----------|-----------|
| **Command** | Delegation | Aggregate Root & DCB | Downstream Activity |
| **Event** | Reaction | Event Processing | Projection |
| **State** | Task Processing | Event Generator | State Transformation |

## Quick install (zero infrastructure)

Packages are not on nuget.org yet (`0.1.0-alpha`). Clone this repository and
add project references to `src/LawnDart` and `src/LawnDart.EventSourcing`, or
pack to a local feed (see the root README).

Then `AddLawnDart` and `AddBoundedContext("default").UseInMemory()`.

## Start here

1. [Start Here](docs/START_HERE.md)
2. [Quickstart](docs/QUICKSTART.md)
3. [Learning Path](docs/learning-path/README.md)
4. [Eventhesis compile contract](docs/EVENTHESIS.md)
