# Schema initialization

Schema setup is an administrative step. Run it once per deploy from a deploy
job, a migration step, or an admin tool. That step uses a connection that can
change the schema. Normal app startup should not run it.

## What goes wrong at startup

A process that calls setup on every start writes the schema from the version
it is running. A host that restarts an older copy, or starts a node that
still runs older code, runs that older setup against a database the newer
version already upgraded.

An older Postgres copy that runs `PostgresEventStore.InitializeSchemaAsync`
drops `append_to_stream` and creates that function again from its own version.

An older copy can append events and write no outbox rows. The store does not
report that.

Postgres projection setup replaces `save_projection_flush` with the function
from the version that is running. An older copy puts its own function back.
The lease table `projection_runner_leases` is created by that same setup.
`PostgresProjectionRunnerLock` has no setup method of its own.

## Order

1. Upgrade the schema with the new version. Run the setup calls from that
   version. Leave `EnableOutbox` false on every copy that is still running.
2. Make sure no older copy can start. A restart of an older copy runs that
   copy's schema setup.
3. Turn on `EnableOutbox`, and any other new feature that needs the new
   schema, only after that.

On a new database, run the new version's event store setup before an older
event store runs. The 0.7.0-alpha.1 event store cannot initialize a schema
that the new inbox created first.

## Registration and startup

`UsePostgres` and `UseSqlServer` register the store for the bounded context.
Table creation stays in the setup calls below. `WithSnapshots` registers
snapshot storage. The event store setup creates the snapshot tables.

`AddPostgresProjectionStores`, `AddSqlProjectionStores`,
`AddPostgresInboxStore`, and `AddSqlInboxStore` register their stores. The
matching initialize call creates the tables.

`LawnDart.AspNetCore` maps web request commands. Schema setup stays in the deploy
step. Startup has no switch for schema setup, because the host does not run
it.

These calls read a format stamp and then write the objects from the version
that is running: `PostgresEventStore.InitializeSchemaAsync`,
`PostgresOutboxWriter.InitializeSchemaAsync`,
`SqlServerOutboxWriter.InitializeSchemaAsync`,
`PostgresInboxStore.InitializeSchemaAsync`, and
`SqlServerInboxStore.InitializeSchemaAsync`.
`SqlServerEventStore.InitializeSchemaAsync` reads the events stamp. A stamp
the call does not accept stops that call.

Projection setup on Postgres and on SQL Server has no stamp. It creates
objects that are missing. Postgres projection setup also replaces
`save_projection_flush`.

## Calls

### Postgres event store

Package: `LawnDart.EventSourcing.Postgres`.

| Call | What it writes |
|---|---|
| `PostgresEventStore.InitializeSchemaAsync` | Schema, events, streams, tags, `append_to_stream`, snapshot tables, and the outbox table |
| `PostgresOutboxWriter.InitializeSchemaAsync` | Outbox table |
| `PostgresSnapshotStore.InitializeSchemaAsync` | `event_snapshots` and `dcb_snapshots` |

The event store call also creates the snapshot tables and the outbox table.
Call the snapshot store or the outbox writer when that object is set up on
its own.

`MigrationConnectionString` is the connection that changes the schema. When
it is unset, `ConnectionString` does that work.

### SQL Server event store

Package: `LawnDart.EventSourcing.SqlServer`.

| Call | What it writes |
|---|---|
| `SqlServerEventStore.InitializeSchemaAsync` | Schema, events, streams, tags, and snapshot tables |
| `SqlServerOutboxWriter.InitializeSchemaAsync` | Outbox table |
| `SqlServerSnapshotStore.InitializeSchemaAsync` | `EventSnapshots` and `DcbSnapshots` |

The event store call also creates the snapshot tables. The outbox writer is
a separate call. Use a connection that can change the schema.

### Inbox

| Call | Package |
|---|---|
| `PostgresInboxStore.InitializeSchemaAsync`, `InitializePostgresInboxStoreAsync` | `LawnDart.Messaging.Postgres` |
| `SqlServerInboxStore.InitializeSchemaAsync`, `InitializeSqlInboxStoreAsync` | `LawnDart.Messaging.SqlServer` |

On Postgres, `MigrationConnectionString` creates the table when it is set.

### Views, checkpoints, and the projection lock

| Call | Package |
|---|---|
| `InitializePostgresProjectionStoresAsync`, `PostgresProjectionStore.InitializeSchemaAsync`, `PostgresViewStore.InitializeSchemaAsync`, `PostgresCheckpointStore.InitializeSchemaAsync` | `LawnDart.Projections.Lightweight.Postgres` |
| `InitializeSqlProjectionStoresAsync`, `SqlViewStore.InitializeSchemaAsync`, `SqlCheckpointStore.InitializeSchemaAsync` | `LawnDart.Projections.Lightweight.SqlServer` |

On Postgres, the view call and the checkpoint call each create
`projection_views`, `projection_checkpoints`, `projection_runner_leases`, and
`save_projection_flush`. SQL Server setup creates `ProjectionViews` and
`ProjectionCheckpoints`.

`IOutboxWriter.InitializeSchemaAsync` is the shared outbox call.
`InMemoryOutboxWriter.InitializeSchemaAsync` completes without a database.

## Deploy command

This program is the deploy step. It is not the app. It uses the Postgres
types. The SQL Server command is the same shape: `SqlServerEventStore`,
`SqlServerOutboxWriter`, `InitializeSqlProjectionStoresAsync`, and
`InitializeSqlInboxStoreAsync`.

```csharp
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Outbox;
using LawnDart.Messaging;
using LawnDart.Messaging.Postgres;
using LawnDart.Projections.Lightweight.Postgres;
using Microsoft.Extensions.Options;

var storeOptions = new PostgresEventStoreOptions
{
    ConnectionString = appConnectionString,
    MigrationConnectionString = adminConnectionString
};

var store = new PostgresEventStore(appConnectionString, options: storeOptions);
await store.InitializeSchemaAsync();

var outbox = new PostgresOutboxWriter(
    appConnectionString,
    schemaName: storeOptions.SchemaName,
    migrationConnectionString: adminConnectionString);
await outbox.InitializeSchemaAsync();

var projectionOptions = new PostgresProjectionStoreOptions
{
    ConnectionString = appConnectionString,
    MigrationConnectionString = adminConnectionString,
    SchemaName = storeOptions.SchemaName
};
var views = new PostgresViewStore(projectionOptions);
var checkpoints = new PostgresCheckpointStore(projectionOptions, views);
await new PostgresProjectionStore(views, checkpoints).InitializeSchemaAsync();

var inbox = new PostgresInboxStore(
    Options.Create(new PostgresInboxStoreOptions
    {
        ConnectionString = appConnectionString,
        MigrationConnectionString = adminConnectionString,
        SchemaName = storeOptions.SchemaName
    }),
    Options.Create(new MessagingOptions()));
await inbox.InitializeSchemaAsync();
```

A host that already called `AddPostgresProjectionStores` and
`AddPostgresInboxStore` can call `InitializePostgresProjectionStoresAsync`
and `InitializePostgresInboxStoreAsync` on that provider instead of
constructing those stores.

## Startup check

`VerifySchemaAsync` reads the format stamp and the tables, functions, and
triggers this version writes. It does not create, replace, or alter them.
Call it when the process starts. `ThrowIfMismatch` throws
`SchemaMismatchException` when the result does not match.

Postgres setup writes the marker `LawnDart:append_to_stream:1` into
`append_to_stream`, and `LawnDart:save_projection_flush:1` into
`save_projection_flush`. The check compares the whole function body with the
body this version writes, and it reads the marker version as an exact number.
Version 1 does not match version 2 or version 10. A body that keeps the marker
and changes anything else does not match. The schema contains that one
function. An extra overload does not match. That includes a function an older
copy put back. The missing list names that function with ` body` after the name.

`InMemoryOutboxWriter.VerifySchemaAsync` returns a match. That writer has no
database. `IOutboxWriter` includes the same method.

### Postgres

| Call | What it reads |
|---|---|
| `PostgresEventStore.VerifySchemaAsync`, `VerifyPostgresEventStoreAsync` | Events stamp, tables, hash partitions, unique keys, `ix_streams_stream_id`, the position sequence, `append_to_stream`, `sync_stream_tags`, and `streams_sync_tags` |
| `PostgresOutboxWriter.VerifySchemaAsync` | Outbox stamp, the outbox table, and `ix_outbox_pending` |
| `PostgresSnapshotStore.VerifySchemaAsync` | `event_snapshots` and `dcb_snapshots` |
| `PostgresViewStore.VerifySchemaAsync`, `PostgresCheckpointStore.VerifySchemaAsync`, `PostgresProjectionStore.VerifySchemaAsync`, `VerifyPostgresProjectionStoresAsync` | Projection tables and `save_projection_flush` |
| `PostgresInboxStore.VerifySchemaAsync`, `VerifyPostgresInboxStoreAsync` | Inbox stamp, the inbox table, and `ix_inbox_processed_at` |

The event store, the outbox writer, the snapshot store, the projection stores,
and the inbox use `ConnectionString`. They do not use
`MigrationConnectionString`.

A login that only reads needs `CONNECT` on the database, `USAGE` on the
schema, and `SELECT` on `schema_info`. The check also reads `pg_catalog`
(`pg_class`, `pg_proc`, `pg_trigger`, and `pg_type`). A normal login can read
that catalog. Snapshot and projection checks do not read `schema_info`. They
need `USAGE` on the schema.

### SQL Server

| Call | What it reads |
|---|---|
| `SqlServerEventStore.VerifySchemaAsync`, `VerifySqlServerEventStoreAsync` | Events stamp `LawnDart_SchemaFormat`, tables, `UX_EventId`, `UX_SequencePosition`, and, when the registry is on, `StreamIdBin`, `UX_StreamIdBin`, and the stream tag trigger |
| `SqlServerOutboxWriter.VerifySchemaAsync` | Outbox stamp `LawnDart_OutboxSchemaFormat` and the outbox table |
| `SqlServerSnapshotStore.VerifySchemaAsync` | `EventSnapshots` and `DcbSnapshots` |
| `SqlViewStore.VerifySchemaAsync`, `SqlCheckpointStore.VerifySchemaAsync`, `VerifySqlProjectionStoresAsync` | `ProjectionViews` and `ProjectionCheckpoints` |
| `SqlServerInboxStore.VerifySchemaAsync`, `VerifySqlInboxStoreAsync` | Inbox stamp `LawnDart_InboxSchemaFormat` and the inbox table |

A login that only reads needs a database user. Grant `VIEW DEFINITION` on
the schema to that user. That grant shows the objects and the format stamp.
The check does not read table rows. Without the grant, the check throws and
says the login lacks permission to read the schema definition. When the
schema is not there, the message says the schema does not exist, or the
login cannot read its definition.

## App startup

After no older copy can start, the app turns `EnableOutbox` on. It does not
call a setup method. It calls the check.

```csharp
using LawnDart;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventStore;
using LawnDart.Messaging.Postgres;
using LawnDart.Projections.Lightweight.Postgres;

[EventTypeName("counter-created")]
public sealed record CounterCreated(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;

services.AddLawnDart(o => o.RequireTenantId = false);
services.AddPostgresProjectionStores("default", appConnectionString);
services.AddPostgresInboxStore(appConnectionString);
services.AddBoundedContext("default")
    .UsePostgres(o =>
    {
        o.ConnectionString = appConnectionString;
        o.RequireTenantId = false;
        o.EnableOutbox = true;
    })
    .WithEventTypes<CounterCreated>();
```

`UseSqlServer` is the same registration for SQL Server. It does not call
`InitializeSchemaAsync`.

After the host is built, read the schema through the registered stores.
`provider` is that built service provider.

```csharp
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Outbox;
using LawnDart.Messaging.Postgres;
using LawnDart.Outbox;
using LawnDart.Projections.Lightweight.Postgres;
using Microsoft.Extensions.DependencyInjection;

(await provider.VerifyPostgresEventStoreAsync("default")).ThrowIfMismatch();

var outbox = (PostgresOutboxWriter)provider.GetRequiredKeyedService<IOutboxWriter>("default");
(await outbox.VerifySchemaAsync()).ThrowIfMismatch();

(await provider.VerifyPostgresProjectionStoresAsync("default")).ThrowIfMismatch();
(await provider.VerifyPostgresInboxStoreAsync()).ThrowIfMismatch();
```

`VerifySqlServerEventStoreAsync` is the same call for a SQL Server context.
It uses the options registered for that context. The other SQL Server calls
are `SqlServerOutboxWriter.VerifySchemaAsync`, `VerifySqlProjectionStoresAsync`,
and `VerifySqlInboxStoreAsync`.

## Related

- [Outbox](OUTBOX_PATTERN.md)
- [Postgres event store](packages/eventsourcing-postgres.md)
- [SQL Server event store](packages/eventsourcing-sqlserver.md)
- [Postgres inbox](packages/messaging-postgres.md)
- [SQL Server inbox](packages/messaging-sqlserver.md)
- [Postgres projections](packages/projections-lightweight-postgres.md)
- [SQL Server projections](packages/projections-lightweight-sqlserver.md)
