# LawnDart.EventSourcing

Event sourcing runtime for LawnDart. Provides aggregate repositories, DCB
repositories, an in-memory event store, JSON serialization, and outbox
processing.

## What's included

- **`AggregateRepository`**: load and save aggregates via any `IEventStore`
- **`DcbRepository`**: Dynamic Consistency Boundary repository
- **`InMemoryEventStore`**: in-process recorded-event log + portable subscriptions (serialize on append)
- **`JsonEventSerializer`**: default `IEventSerializer` (STJ UTF-8 bytes)
- **`UseInMemory()`**: documented default backend on `AddBoundedContext`; registers the in-memory event store, snapshot store, and outbox writer. `EnableOutbox = true` records an outbox message for each event the context declares with `PublishEvents`. `Translate` is the normal case. `Publish<T>()` sends an event that already lives in a contracts project. There is no publish-everything option
- **`InMemorySnapshotStore`**: process-local `ISnapshotStore` / `IDcbSnapshotStore` / `ISnapshotAdmin`
- **`InMemoryOutboxWriter`**: process-local `IOutboxWriter`
- **Snapshot writes**: committed state is captured on the calling thread, then a hosted consumer persists it (see `docs/SNAPSHOTS.md`)
- **`EventSourcingRepositories` / `AddSnapshotWriteInfrastructure`**: public seam for a third-party `Use*` so snapshot writes still enqueue

## Installation

```bash
dotnet add package LawnDart --prerelease
dotnet add package LawnDart.EventSourcing --prerelease
```

## Getting started

```csharp
services.AddLawnDart(o => o.Tenancy = Tenancy.Single);
services.AddBoundedContext("default").UseInMemory();
```

`InMemoryEventStoreOptions.Tenancy` defaults to `Tenancy.Shared`. `UseInMemory` copies `Tenancy`, `SingleTenantId`, and `ValidateNames` onto the store. `Tenancy.Shared` requires a tenant id on appends and `Query.ForTenant(...)` or `Query.ForAllTenants(...)` on queries. Set `Tenancy = Tenancy.Single` (with optional `SingleTenantId`) to run a single-tenant store. `Tenancy.Isolated` is refused at startup. The obsolete `RequireTenantId` property remains as an alias (`true` maps to `Shared`, `false` to `Single`).
