# LawnDart

Core interfaces, abstractions, and building blocks for the LawnDart runtime.
This is the foundation package. All other LawnDart packages depend on it.

## What's included

- **`IEventStore`**: the typed session interface. Backends implement `IEventLog`; `UseInMemory()` and `UseSqlServer()` live in the EventSourcing packages, not here
- **`IEvent` / `ICommand` / `IState`**: base interfaces for your domain types
- **`AggregateRoot`**: base class for aggregate roots; apply events and track uncommitted changes
- **`IAggregateRepository`**: load and save aggregates against any `IEventStore` implementation
- **`IBoundedContextEventStore`**: named handle to a full context stack
- **`BoundedContextBuilder`**: fluent builder returned by `services.AddBoundedContext(contextName)`
- **`IDcbRepository`**: Dynamic Consistency Boundary (DCB) pattern support
- **`IReactor` / `IEventProcessor` / `ITaskProcessor`**: hosted pattern contracts
- **`ICommandHandler`**: app-facing command dispatch
- **`ICommandDispatcher`**: routes reactor/processor commands to `ICommandHandler<T>` (same `LawnDart.Messaging` namespace as `MessageContext`; EventSourcing registers `ContextAwareCommandDispatcher`)
- **Snapshots**: `ISnapshotStore`, `IDcbSnapshotStore`, `ISnapshotStrategy`, built-in strategies
- **Authorization**: `IAuthorizationProvider`, attributes, `AuthorizationService`
- **Metadata**: `IMetadataProvider`, `ITenantContextProvider`, `EventMetadata`, `CommandMetadata`. See [Metadata](https://github.com/sellistixorg/LawnDart/blob/main/docs/METADATA.md)
- **Tagging**: `ITagProvider`, `[Tag]` attribute
- **Serialization**: `IEventSerializer` (`ReadOnlyMemory<byte>`), `[PropertyOrder]`
- **Event session**: `EventSession` maps `IEvent` to `AppendEvent` / `RecordedEvent`
- **Outbox**: `IOutboxWriter`, `IOutboxPublisher`, `OutboxMessage`

Author Lightweight views as `ProjectionBase<TView>` plus scope attributes
in `LawnDart.Projections.Lightweight`. Shapes:
[CES matrix](https://github.com/sellistixorg/LawnDart/blob/main/docs/CES_MATRIX.md).

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

- `Microsoft.Extensions.DependencyInjection`
- `Microsoft.Extensions.Logging.Abstractions`
- `Microsoft.Extensions.Options`
