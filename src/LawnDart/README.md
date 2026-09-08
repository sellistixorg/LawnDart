# LawnDart

Core interfaces, abstractions, and building blocks for the LawnDart runtime.
This is the foundation package — all other LawnDart packages depend on it.

## What's included

- **`IEventStore`** — primary abstraction for reading and appending events
- **`IEvent` / `ICommand` / `IMessage` / `IState`** — base marker interfaces for your domain types
- **`AggregateRoot`** — base class for aggregate roots; apply events and track uncommitted changes
- **`IAggregateRepository`** — load and save aggregates against any `IEventStore` implementation
- **`IBoundedContextEventStore`** — named handle to a full context stack
- **`BoundedContextBuilder`** — fluent builder returned by `services.AddBoundedContext(contextName)`
- **`IDcbRepository`** — Dynamic Consistency Boundary (DCB) pattern support
- **`IReactor` / `IProjector` / `IEventProcessor`** — patterns for reacting to and projecting events
- **`ICommandHandler` / `ICommandDelegator`** — command dispatch abstractions
- **`ITaskProcessor`** — background task processing pattern
- **Snapshots** — `ISnapshotStore`, `IDcbSnapshotStore`, `ISnapshotStrategy`, built-in strategies
- **Authorization** — `IAuthorizationProvider`, attributes, `AuthorizationService`
- **Metadata** — `IMetadataProvider`, `ITenantContextProvider`, `EventMetadata`, `CommandMetadata`
- **Tagging** — `ITagProvider`, `[Tag]` attribute
- **Serialization** — `IEventSerializer`, `[PropertyOrder]`
- **Outbox** — `IOutboxWriter`, `IOutboxPublisher`, `OutboxMessage`

## Installation

```bash
dotnet add package LawnDart --prerelease
```

## Getting started

```csharp
services.AddLawnDart();
```

Optionally configure options and provide a tenant context:

```csharp
services.AddLawnDart(options =>
{
    options.RequireTenantId = false;
});

services.AddTenantContextProvider<MyTenantContextProvider>();
```

## Package dependencies

- [`MemoryPack`](https://github.com/Cysharp/MemoryPack)
- `Microsoft.Extensions.DependencyInjection`
- `Microsoft.Extensions.Logging.Abstractions`
- `Microsoft.Extensions.Options`
