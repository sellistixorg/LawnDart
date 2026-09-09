---
_layout: landing
---

# LawnDart

In-process .NET library for event-sourced commands, events, and state.

LawnDart implements the **nine common patterns** — a command–event–state
matrix. Five cells are hosted; four are planned interfaces with no host.

## Pattern matrix

| From \ To | **Command** | **Event** | **State** |
|-----------|-------------|-----------|-----------|
| **Command** | Delegation 🔧 | Aggregate Root & DCB ✅ | Downstream Activity 🔧 |
| **Event** | Reaction ✅ | Event Processing ✅ | Projection ✅ |
| **State** | Task Processing ✅ | Event Generator 🔧 | State Transformation 🔧 |

✅ Hosted (runtime, DI, tests) · 🔧 Planned interface — `[Experimental]`, no host yet. Implementing a 🔧 type does not register or run it.

## Quick install (zero infrastructure)

```bash
dotnet add package LawnDart --prerelease
dotnet add package LawnDart.EventSourcing --prerelease
```

Then `AddLawnDart` and `AddBoundedContext("default").UseInMemory()`.

## Start here

1. [Start Here](docs/START_HERE.md)
2. [Quickstart](docs/QUICKSTART.md)
3. [Learning Path](docs/learning-path/README.md)
4. [Eventhesis and LawnDart](docs/EVENTHESIS.md)
