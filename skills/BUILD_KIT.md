# LawnDart build kit

This file is the **only entry point** an agent needs. Clone the repo (or check
out the git tag that matches the LawnDart packages you consume), open this
file, and follow the reading order. Do not invent types that are not in a
skill's frozen surface.

The kit lives in this repository. Its version **is the git tag**. There is no
NuGet package and no `dotnet new` template.

Targets LawnDart 0.4

CI fails when MinVer's `major.minor` is not `0.4`. Height-suffixed versions
such as `0.4.0-alpha.2.1` and `0.4.1-alpha.0.7` still match. A `0.5` tag does
not — update this line when the kit is reviewed for that minor.

## Generic slice spec

The kit is written against a **modelling-tool-neutral** slice:

| Concept | Meaning |
|---|---|
| Command | Intent to change something. One handler. |
| Event | Immutable fact already committed. Catalog token is stable. |
| Entity | Consistency boundary that owns decision state (aggregate or DCB). |
| View | Read model folded from events. Not the entity's decision state. |
| Process | Reaction or follow-on work after events (optional). |
| GWT | Given / when / then proof that the slice behaves. |
| Vertical slice | One named bounded context that hosts the above. |

Skills teach how those concepts become LawnDart types. They never name a
modelling tool.

A modelling tool is an **adapter** that maps its export onto this spec:

| Adapter | Input | Doc |
|---|---|---|
| Eventhesis (first) | Slice-based JSON event model | [docs/EVENTHESIS.md](../docs/EVENTHESIS.md) |

Adding prooph board, eventmodelers.ai, or another canvas is one new adapter
doc and **zero skill changes**.

## Reading order

1. This file.
2. The adapter for the model you were given (Eventhesis → `docs/EVENTHESIS.md`).
   Skip if the input is already the generic spec.
3. Skills, in this order:
   1. [`lawndart-host-setup`](lawndart-host-setup/SKILL.md) — hub, packages, registration order
   2. [`lawndart-event-store`](lawndart-event-store/SKILL.md) — `UseInMemory` / `UseSqlServer`
   3. [`lawndart-bounded-context`](lawndart-bounded-context/SKILL.md) — named contexts, handlers
   4. [`lawndart-domain-model`](lawndart-domain-model/SKILL.md) — commands, events, aggregates, DCB
   5. [`lawndart-lightweight-projections`](lawndart-lightweight-projections/SKILL.md) — host the read model
   6. [`lawndart-projection-authoring`](lawndart-projection-authoring/SKILL.md) — projection classes
   7. [`lawndart-aspnet-hosting`](lawndart-aspnet-hosting/SKILL.md) — HTTP commands and auth
4. Required docs:
   - [docs/DI_GRAMMAR.md](../docs/DI_GRAMMAR.md)
   - [docs/HTTP_COMMANDS.md](../docs/HTTP_COMMANDS.md)
   - [docs/testing/BDD_TESTING.md](../docs/testing/BDD_TESTING.md)
5. Reference slice (lands in `RDY-10`): `samples/Library.Domain` and
   `samples/Library.Domain.Tests`. Until those projects exist, Academy
   (`demos/LawnDart.Demo.Academy` / `Academy.WebApi`) is the runnable host,
   not the excerpt source.

Supporting material (not required to implement a slice): the
[learning path](../docs/learning-path/README.md) and per-package pages under
[docs/packages/](../docs/packages/README.md).

## Rules

- Register handlers with `WithCommandHandlers<TMarker>()` on the bounded
  context. `AddLawnDartHttpCommands` only maps routes.
- Aggregates and DCB entities declare closed `Handle(TCommand)`. Persist with
  `HandleCommandAsync`. Do not implement Delegation, Downstream Activity,
  Event Generator, or State Transformation — those cells are not hosted.
- After `RDY-10`, every code block in `skills/` and `docs/` is excerpted from
  the reference slice. Until then, snippets may still be hand-written; edited
  blocks carry `TODO(RDY-10)`.
- Consume the LawnDart package version that matches the tag you cloned.
  Patterns pins `LawnDartPackageVersion`; generating from `main` against an
  older pin is the drift this `Targets` line exists to catch.
