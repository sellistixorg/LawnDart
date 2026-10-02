# LawnDart.EventSourcing

Event sourcing runtime for LawnDart. Provides aggregate repositories, DCB
repositories, an in-memory event store, JSON serialization, and outbox
processing.

## What's included

- **`AggregateRepository`**: load and save aggregates via any `IEventStore`
- **`DcbRepository`**: Dynamic Consistency Boundary repository
- **`InMemoryEventStore`**: in-process recorded-event log + portable subscriptions (serialize on append)
- **`JsonEventSerializer`**: default `IEventSerializer` (STJ UTF-8 bytes)
- **`UseInMemory()`**: documented default backend on `AddBoundedContext`; registers the in-memory event store, snapshot store, and outbox writer
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
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default").UseInMemory();
```
