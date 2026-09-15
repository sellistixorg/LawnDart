# Step 1 — Core concepts

**Next:** [Step 2 — First aggregate](02-first-aggregate.md)

## Command, event, state

Every message-driven system shuffles three kinds of thing:

- **Command**: intent to change something in the future.
- **Event**: immutable fact about what already happened.
- **State**: current read model used by users and workflows.

You do not need all nine From×To combinations on day one. Most apps start
with aggregate + projection.

The shapes an event model compiles into — five hosted today — are on the
[CES matrix](../CES_MATRIX.md).

## What the hosted shapes do

**Aggregate Root (Command → Event)** — A command is checked, then events are
appended. State is rebuilt by replaying those events.

**Projection (Event → State)** — Events reduce into a query-friendly view.

**Reaction (Event → Command)** — An event in one slice becomes a command in
another (choreography).

**Event Processing (Event → Event)** — Transform or enrich events without a
command. Same host family as reactions (step 6).

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
