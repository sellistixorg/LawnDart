# Extension method index

Happy-path registrations shipped in LawnDart v1.

Host grammar (the frozen surface) is on [DI Grammar](DI_GRAMMAR.md).

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
| `AddTagProvider<T>` | `LawnDart` | Default or named-context `ITagProvider` |
| `AddTagFormat<T>` | `LawnDart` | Default or named-context `ITagFormat` |
| `AddBoundedContext` | `LawnDart` | Named/keyed context builder |
| `AddLawnDartAuthorization` | `LawnDart` | Core `AuthorizationService` |
| `AddCompositeAuthorizationContext` | `LawnDart` | Compose leaf context providers |
| `AddHttpAuthorizationContext` | `LawnDart.Authorization.AspNetCore` | HTTP claims → `AuthorizationContext` |

## Event store

| Method | Package | Purpose |
|---|---|---|
| `UseInMemory` | `LawnDart.EventSourcing` | Process-local store, snapshot store, and outbox writer; registers `ICommandDispatcher` (interface lives in Core) |
| `UseSqlServer` | `LawnDart.EventSourcing.SqlServer` | Durable SQL store |
| `UsePostgres` | `LawnDart.EventSourcing.Postgres` | Durable Postgres store |
| `WithSnapshots` | `LawnDart.EventSourcing.SqlServer` | Optional SQL Server snapshot store |
| `WithSnapshots` | `LawnDart.EventSourcing.Postgres` | Optional Postgres snapshot store |
| `AddSnapshotWriteInfrastructure` | `LawnDart.EventSourcing` | Snapshot write channel + hosted consumer. `UseInMemory` / `UseSqlServer` already call it; third-party `Use*` methods must |
| `EventSourcingRepositories.Create*` | `LawnDart.EventSourcing` | Build repositories with the write queue. Public constructors omit the queue. |
| `WithCommandHandlers` / `WithCommandHandlers<TMarker>` | `LawnDart.EventSourcing` | The only handler registrar. Scan `ICommandHandler<T>` and register `ICommandDispatcher` (`LawnDart.Messaging` namespace, Core package) |
| `WithTagProvider` | `LawnDart.EventSourcing` | Context `ITagProvider` |
| `WithTagFormat` | `LawnDart.EventSourcing` | Context `ITagFormat` |

## Projections

| Method | Package | Purpose |
|---|---|---|
| `AddInMemoryProjectionStores` | `LawnDart.Projections.Lightweight` | Dev/test view + checkpoint stores |
| `AddSqlProjectionStores` | `LawnDart.Projections.Lightweight.SqlServer` | Durable SQL Server view + checkpoint stores |
| `InitializeSqlProjectionStoresAsync` | `LawnDart.Projections.Lightweight.SqlServer` | Create the view and checkpoint tables |
| `AddPostgresProjectionStores` | `LawnDart.Projections.Lightweight.Postgres` | Durable Postgres view and checkpoint stores, and the single-runner lock |
| `InitializePostgresProjectionStoresAsync` | `LawnDart.Projections.Lightweight.Postgres` | Create the view and checkpoint tables |
| `WithProjections` | `LawnDart.Projections.Lightweight` | Scan projectors for a context |
| `MapProjectionQueries` | `LawnDart.Projections.Lightweight` | HTTP GET views |

## Messaging

| Method | Package | Purpose |
|---|---|---|
| `AddMessaging` | `LawnDart.Messaging` | Reactor / processor host, transport options, and the startup check |
| `AddMessagePreHandler<THandler>` | `LawnDart.Messaging` | Hook that runs before each handler attempt |
| `AddInMemoryMessaging` | `LawnDart.Messaging.InMemory` | In-process transport |
| `AddRabbitMqTransport` | `LawnDart.Messaging.RabbitMQ` | RabbitMQ transport, prefetch, retries, and a dead-letter exchange |
| `AddAzureServiceBusTransport` | `LawnDart.Messaging.AzureServiceBus` | Azure Service Bus transport, topics, retries, and subscription dead-letter |
| `AddAwsSqsTransport` | `LawnDart.Messaging.AwsSqs` | Amazon SQS transport, SNS fan-out, retries, and a per-subscription dead-letter queue |
| `AddSqlInboxStore` | `LawnDart.Messaging.SqlServer` | Durable inbox. Replaces the in-memory inbox in either call order |
| `InitializeSqlInboxStoreAsync` | `LawnDart.Messaging.SqlServer` | Create the inbox table |
| `AddPostgresInboxStore` | `LawnDart.Messaging.Postgres` | Durable Postgres inbox. Replaces the in-memory inbox in either call order |
| `InitializePostgresInboxStoreAsync` | `LawnDart.Messaging.Postgres` | Create the Postgres inbox table |
| `AddReactor<TReactor, TEvent>` | `LawnDart.Messaging` | Event → command |
| `AddEventProcessor<TProcessor, TEvent>` | `LawnDart.Messaging` | Event → event / work |
| `AddTaskProcessor<TProcessor>` | `LawnDart.Messaging` | State → command |
| `AddMessageTransportOutboxPublisher` | `LawnDart.Messaging` | Outbox → transport |

## HTTP

| Method | Package | Purpose |
|---|---|---|
| `AddLawnDartHttpCommands` | `LawnDart.AspNetCore` | Record assemblies for HTTP routing (does not register handlers) |
| `MapLawnDartCommands` | `LawnDart.AspNetCore` | Map POST endpoints |
