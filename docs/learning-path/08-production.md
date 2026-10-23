# Step 8. Production shape

**Previous:** [Testing EDA](07-testing-eda.md)

Swap InMemory for SQL Server or Postgres. Keep the same bounded-context name.

The verified swap is command dispatch, event persistence, aggregate reload,
projection materialisation, and read-back. Outbox and subscriptions have
their own tests. A green swap does not prove those paths.

You also change projection stores (`AddInMemoryProjectionStores` to
`AddSqlProjectionStores` in `LawnDart.Projections.Lightweight.SqlServer`) and
run schema init from a deploy or admin step (`InitializeSchemaAsync` /
`InitializeSqlProjectionStoresAsync`). See
[Schema initialization](../SCHEMA_INITIALIZATION.md). Add
`using LawnDart.Projections.Lightweight.SqlServer`. Postgres uses
`AddPostgresProjectionStores` and `InitializePostgresProjectionStoresAsync`
in `LawnDart.Projections.Lightweight.Postgres`. SQL Server and Postgres
snapshots are opt-in (`WithSnapshots` on that store's builder). Academy's `SqlServer` launch profile is the local Windows
path (`Trusted_Connection=True`); see [BACKEND_SELECTION.md](../BACKEND_SELECTION.md).

Host grammar: [DI Grammar](../DI_GRAMMAR.md).

The store snippet below leaves the message transport to you.
`AddMessageTransportOutboxPublisher` publishes outbox rows through the
registered `IMessageTransport`. Register `AddInMemoryMessaging`,
`AddRabbitMqTransport`, or `AddAzureServiceBusTransport` before that call.
See [Message transports](../MESSAGE_TRANSPORTS.md) and
[Outbox](../OUTBOX_PATTERN.md).

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddSqlProjectionStores("default", cs);
services.AddBoundedContext("default")
    .UseSqlServer(o =>
    {
        o.ConnectionString = cs;
        o.RequireTenantId = false;
        o.EnableOutbox = true;
    })
    .WithCommandHandlers<RegisterStudentCommandHandler>()
    .WithEventTypes<StudentRegistered>()
    .WithProjections([typeof(StudentSummaryProjection).Assembly]);

services.AddLawnDartHttpCommands(typeof(RegisterStudentCommand).Assembly);
services.AddHttpAuthorizationContext();
app.MapLawnDartCommands();
app.MapProjectionQueries("default");
```

Postgres is the same swap. `UsePostgres`, `AddPostgresProjectionStores`, and
`InitializePostgresProjectionStoresAsync` are the calls. Schema setup stays
in the deploy step. See [Postgres hosting](../POSTGRES_HOSTING.md).

## Schema deploy

Additive JSON rolls freely: new named properties on the current type do
not require a `SchemaVersion` bump. A breaking change (renamed meaning, a
removed required field, or a remapped `[PropertyOrder]` number) ships
in two steps. Ship readers that understand `SchemaVersion` N+1 before
any process writes N+1. Old binaries throw `EventSchemaTooNewException`
on those newer rows and leave them unread. They may keep appending the
version that was current for them. There is no remote downcaster and no
skip override.

See [Event schema versioning](../EVENT_SCHEMA_VERSIONING.md). Envelope
fields: [Metadata](../METADATA.md).

## Wipe, then recreate

Drop and recreate SQL event, outbox, and inbox tables, then run
`InitializeSchemaAsync` and, when the inbox is SQL Server,
`InitializeSqlInboxStoreAsync`, from that same deploy step. Opening an
older table throws and names drop-and-recreate. Setup makes additive
changes in place, and there is no separate migration tool.

Checklist:

- [BACKEND_SELECTION.md](../BACKEND_SELECTION.md): InMemory vs SQL
- [OUTBOX_PATTERN.md](../OUTBOX_PATTERN.md): durable publish
- [MESSAGE_TRANSPORTS.md](../MESSAGE_TRANSPORTS.md): in-process, RabbitMQ, Azure Service Bus
- [AUTHORIZATION.md](../AUTHORIZATION.md): HTTP claims
- [HTTP_COMMANDS.md](../HTTP_COMMANDS.md): POST mapping
- [EVENT_SCHEMA_VERSIONING.md](../EVENT_SCHEMA_VERSIONING.md): event names, upcast, ship readers first
