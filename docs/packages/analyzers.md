# LawnDart.Analyzers

Roslyn analyzers for event schema versioning. Package id is
`LawnDart.Analyzers`. Diagnostic prefix `LDT`.

Take it when you want `dotnet build` to fail on a broken version line
without starting a host. It is optional. The runtime still requires
`WithEventTypes` / `WithUpcasters` warmup.

## What you get

- `LDT001` — two `current: true` on one family token
- `LDT002` — two or more types and no `current: true`
- `LDT003` — a historical version has no upcaster path to current

One-arg `[EventTypeName("token")]` is not a diagnostic. Warmup is the
runtime authority if the analyzer and catalog drift. Positioning §3 is
reopened only for these versioning rules.

See the [package README](../../src/LawnDart.Analyzers/README.md) and
[Event schema versioning](../EVENT_SCHEMA_VERSIONING.md).
