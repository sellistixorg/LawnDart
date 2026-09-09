# Extension method index

Happy-path registrations shipped in LawnDart v1.

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs).
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization (`HandleAsync<TCommand>` on the entity is obsolete).
3. **Load** by `string streamId` when the stream is not `{type}:{guid}` — see repository methods below.
4. **Projections:** `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** `UseInMemory` / `UseSqlServer` on `AddBoundedContext(name)`.

## Repository (stream id)

These live on `IAggregateRepository` in Core (not `Add*` extensions). Guid overloads build `{type}:{id}` / `{tenant}:{type}:{id}`.

| Method | Primary use |
|---|---|
| `GetAsync<T>(Guid id)` | Load by aggregate id, or `null` if missing |
| `GetAsync<T>(string streamId)` | Load when the stream is not `{type}:{guid}` |
| `CreateAsync<T>(Guid id)` | New instance; does not check existence |
| `CreateAsync<T>(string streamId)` | New instance with a custom stream id |
| `GetOrCreateAsync<T>(Guid id)` | Preferred load/create for `{type}:{guid}` streams |
| `GetOrCreateAsync<T>(string streamId)` | Preferred load/create for custom stream ids |
| `HandleCommandAsync<T, TCommand>(...)` | Auth + `Handle(TCommand)` + persist |

## Host

| Method | Package | Purpose |
|---|---|---|
| `AddLawnDart` | `LawnDart` | Core options + default `IMetadataProvider` |
| `AddTenantContextProvider<T>` | `LawnDart` | Tenant metadata source |
| `AddMetadataProvider<T>` | `LawnDart` | Replace default metadata |
| `AddTagProvider<T>` | `LawnDart` | Default tag provider |
| `AddBoundedContext` | `LawnDart` | Named/keyed context builder |
| `AddLawnDartAuthorization` | `LawnDart` | Core `AuthorizationService` |
| `AddCompositeAuthorizationContext` | `LawnDart` | Compose leaf context providers |
| `AddHttpAuthorizationContext` | `LawnDart.Authorization.AspNetCore` | HTTP claims → `AuthorizationContext` |

## Event store

| Method | Package | Purpose |
|---|---|---|
| `UseInMemory` | `LawnDart.EventSourcing` | Process-local store; registers `ICommandDispatcher` (interface lives in Core) |
| `UseSqlServer` | `LawnDart.EventSourcing.SqlServer` | Durable SQL store |
| `WithSnapshots` | `LawnDart.EventSourcing.SqlServer` | Optional snapshot store |
| `WithCommandHandlers` | `LawnDart.EventSourcing` | Scan `ICommandHandler<T>` and register `ICommandDispatcher` (`LawnDart.Messaging` namespace, Core package) |
| `WithTagProvider` | `LawnDart.EventSourcing` | Per-context tags |

## Projections

| Method | Package | Purpose |
|---|---|---|
| `AddInMemoryProjectionStores` | `LawnDart.Projections.Lightweight` | Dev/test view + checkpoint stores |
| `AddSqlProjectionStores` | `LawnDart.Projections.Lightweight` | Durable view + checkpoint stores |
| `WithProjections` | `LawnDart.Projections.Lightweight` | Scan projectors for a context |
| `MapProjectionQueries` | `LawnDart.Projections.Lightweight` | HTTP GET views |

## Messaging

| Method | Package | Purpose |
|---|---|---|
| `AddMessaging` | `LawnDart.Messaging` | Reactor / processor host |
| `AddInMemoryMessaging` | `LawnDart.Messaging.InMemory` | In-process transport |
| `AddReactor<TReactor, TEvent>` | `LawnDart.Messaging` | Event → command |
| `AddEventProcessor<TProcessor, TEvent>` | `LawnDart.Messaging` | Event → event / work |
| `AddTaskProcessor<TProcessor>` | `LawnDart.Messaging` | State → command |
| `AddMessageTransportOutboxPublisher` | `LawnDart.Messaging` | Outbox → transport |

## HTTP

| Method | Package | Purpose |
|---|---|---|
| `AddLawnDartHttpCommands` | `LawnDart.AspNetCore` | Discover handlers |
| `MapLawnDartCommands` | `LawnDart.AspNetCore` | Map POST endpoints |
