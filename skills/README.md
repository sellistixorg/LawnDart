# Agent skills

Consumer-facing [Agent Skills](https://agentskills.io/specification) for apps
built on LawnDart NuGet packages.

**Start at [BUILD_KIT.md](BUILD_KIT.md).** That file is the only entry point:
reading order, the generic slice spec, modelling-tool adapters, and
`Targets LawnDart X.Y`. Do not treat this README as the kit.

The seven skills teach LawnDart. They do not name a modelling tool.

| Skill | Purpose |
|---|---|
| `lawndart-host-setup` | Hub: decision tree, packages, registration order |
| `lawndart-event-store` | `UseInMemory` / `UseSqlServer` |
| `lawndart-bounded-context` | Named contexts and command handlers |
| `lawndart-domain-model` | Events, commands, aggregates, DCB, stream IDs |
| `lawndart-lightweight-projections` | `WithProjections`, stores, `MapProjectionQueries` |
| `lawndart-projection-authoring` | Projection classes and view keys |
| `lawndart-aspnet-hosting` | `MapLawnDartCommands`, HTTP auth |
