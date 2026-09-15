---
_layout: landing
---

# LawnDart

LawnDart is the .NET runtime that event-modeled systems compile into.

You model slices — in Eventhesis, in prooph board, in eventmodelers.ai, or on a
whiteboard — and LawnDart is what they become. Written by a human or generated
by an agent.

Supported today: InMemory and SQL Server, in-process messaging, `net10.0`.
[Why LawnDart](docs/WHY.md).

## Quick install (zero infrastructure)

```bash
dotnet add package LawnDart --prerelease
dotnet add package LawnDart.EventSourcing --prerelease
```

Then `AddLawnDart` and `AddBoundedContext("default").UseInMemory()`.
The repository [README](https://github.com/sellistixorg/LawnDart) has the
end-to-end Counter.

## Start here

1. [Why LawnDart](docs/WHY.md)
2. [Start Here](docs/START_HERE.md)
3. [Quickstart](docs/QUICKSTART.md)
4. [Learning Path](docs/learning-path/README.md)
5. [CES matrix](docs/CES_MATRIX.md) — the shapes an event model compiles into; five are hosted today
6. [Eventhesis adapter](docs/EVENTHESIS.md)
