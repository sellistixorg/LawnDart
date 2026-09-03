# Extension method index

Happy-path registrations shipped in LawnDart v1.

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
| `UseInMemory` | `LawnDart.EventSourcing` | Process-local store |
| `UseSqlServer` | `LawnDart.EventSourcing.SqlServer` | Durable SQL store |
| `WithSnapshots` | `LawnDart.EventSourcing.SqlServer` | Optional snapshot store |
| `WithCommandHandlers` | `LawnDart.EventSourcing` | Scan `ICommandHandler<T>` |
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
