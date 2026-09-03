---
name: lawndart-projection-authoring
description: Write LawnDart Lightweight projection classes, view DTOs, and view keys. Use when adding a read model.
---

# Projection authoring

Define a view DTO and a projector that applies events. Use the Lightweight SDK
attributes / `IProjector` contracts in `LawnDart.Projections.Lightweight` and
`LawnDart.Projections.Sdk`.

Academy WebApi examples:

- `StudentSummaryProjection` — per-student stream
- section availability — per-section stream
- enrollment index — global view

Keep projectors deterministic. Do not call the event store from `Apply`.
Put auth on query endpoints with `ProjectionEndpointAttribute` when the view
is tenant-scoped.

See `demos/LawnDart.Demo.Academy.WebApi/Projections`.
