# Agent skills

Consumer-facing [Agent Skills](https://agentskills.io/specification) for apps
built on LawnDart NuGet packages.

**Skills version:** `0.1.0`  
**Minimum package version:** `0.1.0-alpha`

Install Phase 1 + Phase 2 (7 skills) for a greenfield Web API.

## Phase 1 — Host integration

| Skill | Purpose |
|---|---|
| `lawndart-host-setup` | Hub: decision tree, packages, registration order |
| `lawndart-event-store` | `UseInMemory` / `UseSqlServer` |
| `lawndart-bounded-context` | Named contexts and command handlers |
| `lawndart-lightweight-projections` | `WithProjections`, stores, `MapProjectionQueries` |

## Phase 2 — Application code + HTTP

| Skill | Purpose |
|---|---|
| `lawndart-domain-model` | Events, commands, aggregates, DCB, stream IDs |
| `lawndart-projection-authoring` | Projection classes and view keys |
| `lawndart-aspnet-hosting` | `MapLawnDartCommands`, HTTP auth |
