# Step 1 — Core concepts

**Next:** [Step 2 — First aggregate](02-first-aggregate.md)

## The nine common patterns

Every message-driven system shuffles three kinds of thing: **commands** (intent),
**events** (facts), and **state** (present view). Those combine into nine
patterns — the command–event–state matrix. Five are hosted; four are planned
interfaces.

| From \ To | **Command** | **Event** | **State** |
|---|---|---|---|
| **Command** | Delegation 🔧 | Aggregate Root & DCB ✅ | Downstream Activity 🔧 |
| **Event** | Reaction ✅ | Event Processing ✅ | Projection ✅ |
| **State** | Task Processing ✅ | Event Generator 🔧 | State Transformation 🔧 |

✅ Hosted (runtime, DI, tests) · 🔧 Planned interface — `[Experimental]`, no host yet. Implementing a 🔧 type does not register or run it.

You do not need all nine on day one. Most apps start with aggregate + projection
(both ✅).

## What each pattern does

**Aggregate Root (Command → Event)** — A command is checked, then events are
appended. State is rebuilt by replaying those events.

**Projection (Event → State)** — Events reduce into a query-friendly view.

**Reaction (Event → Command)** — An event in one slice becomes a command in
another (choreography).

**Task Processing (State → Command)** — A poller emits commands from read-model
conditions (timeouts, SLAs).

**DCB** — When a rule spans identities, load by tags and append atomically.
See [DCB_PATTERNS.md](../DCB_PATTERNS.md).

## Mental model

```
Command → Aggregate / DCB → Events → Event store
                                    ↓
                         Projector → View store → Query
```

[Glossary](../GLOSSARY.md) · [Overview](../OVERVIEW.md)
