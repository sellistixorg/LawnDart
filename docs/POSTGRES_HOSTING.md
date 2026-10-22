# Postgres hosting

LawnDart stores events in Postgres 15 or newer. A direct connection is enough.
[PgBouncer](https://www.pgbouncer.org/) can sit in front of the app connection.
PgBouncer is a connection pooler.

Schema setup is a deploy or admin step. The app does not create tables when
it starts. See [Schema initialization](SCHEMA_INITIALIZATION.md).

These notes were checked on 22 October 2026.

- Postgres `LISTEN` (register this session for a wake-up): [LISTEN](https://www.postgresql.org/docs/current/sql-listen.html)
- Postgres `NOTIFY` (send that wake-up): [NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)
- PgBouncer pooling modes and the feature map: [Features](https://www.pgbouncer.org/features.html)
- PgBouncer `pool_mode`: [Configuration](https://www.pgbouncer.org/config.html)
- Npgsql and PgBouncer: [Compatibility](https://www.npgsql.org/doc/compatibility.html#pgbouncer)

Npgsql is the .NET driver the Postgres packages use.

## Three connection strings

| Setting | What uses it | Point it at |
|---|---|---|
| `ConnectionString` | Reads, appends, inbox claims, view writes, checkpoint writes | Postgres directly, or PgBouncer. A pooled connection is fine. |
| `ListenConnectionString` | The wake-up listener, and the projection session lock | Postgres directly, or PgBouncer in session mode |
| `MigrationConnectionString` | Schema setup only | Postgres directly, with a role that can create tables |

`UsePostgres`, `AddPostgresProjectionStores`, and `AddPostgresInboxStore`
each have these settings on their options. The inbox has no listener. It
uses `ConnectionString` and `MigrationConnectionString` only.

When `ListenConnectionString` is unset, the listener and the projection
lock use `ConnectionString`. When `MigrationConnectionString` is unset,
schema setup uses `ConnectionString`.

The sample host shows a direct connection. The steps are in
`samples/Library.Host/README.md`.

## Plain Postgres

One connection string is enough. Leave `ListenConnectionString` unset.
The same host accepts `LISTEN`.

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddPostgresProjectionStores("default", connectionString);
services.AddPostgresInboxStore(connectionString);
services.AddBoundedContext("default")
    .UsePostgres(o =>
    {
        o.ConnectionString = connectionString;
        o.RequireTenantId = false;
    });
```

That block is app startup. It does not create tables. Run
`InitializeSchemaAsync`, `InitializePostgresProjectionStoresAsync`, and
`InitializePostgresInboxStoreAsync` from the deploy step. The deploy
program is in [Schema initialization](SCHEMA_INITIALIZATION.md).

## PgBouncer

`pool_mode` chooses when PgBouncer returns a server connection to the pool.
Checked against the PgBouncer feature map on 22 October 2026:

- Session mode keeps the server connection until the client disconnects.
  `LISTEN` works. A session lock works.
- Transaction mode keeps the server connection for one transaction.
  `LISTEN` does not work. `NOTIFY` does: an append can send the wake-up
  through the pooler. A session lock does not work. A lock that lasts only
  for the current transaction still ends with that transaction, so an
  append can take its locks on this connection.

Put the app on the transaction-mode pooler. Set `ConnectionString` to that
pooler. Appends, reads, and inbox claims use it. An inbox claim is one
statement.

Put the listener on Postgres, or on a session-mode pooler. Set
`ListenConnectionString` to that connection. The projection lock uses the
same setting. Set `RunnerLockMode` to `Lease` when that connection is a
transaction pooler. `Auto` (the default) switches to a lease only when the
host contains `pooler` or `pgbouncer`, or the port is `6543`. Any other
host name keeps a session lock. Set `Lease` when your pooler uses a
different name or port. With a lease, the waiting copy takes over after
`LeaseDuration` (default 15 seconds) with no renewal.

Point `MigrationConnectionString` at Postgres directly. Schema setup copies
existing stream tags under a session lock. A transaction pooler cannot keep
that lock. The app role does not need to own the tables.

On the app connection, when Npgsql's own pool stays on and PgBouncer is in
transaction mode or statement mode, Npgsql's compatibility notes say to set
`No Reset On Close=true`. That stops Npgsql from sending `DISCARD ALL` when
it returns a connection to its pool. Checked on 22 October 2026.

```csharp
services.AddBoundedContext("default")
    .UsePostgres(o =>
    {
        o.ConnectionString = pooledConnectionString;
        o.ListenConnectionString = directConnectionString;
        o.MigrationConnectionString = adminConnectionString;
        o.RequireTenantId = false;
    });

services.AddPostgresProjectionStores("default", pooledConnectionString, configure: o =>
{
    o.ListenConnectionString = directConnectionString;
    o.MigrationConnectionString = adminConnectionString;
    o.RunnerLockMode = PostgresRunnerLockMode.Lease;
});
```

Set `RunnerLockMode` to `Lease` only when the lock connection itself is a
transaction pooler. When `ListenConnectionString` is a direct connection,
leave the mode at `Auto`.

## The pooler warning

At startup the listener opens `ListenConnectionString`, or
`ConnectionString` when that is the only one, and sends a probe wake-up.
It logs one warning and then only polls when either of these is true:

- The listen host looks like a transaction pooler. The host contains
  `pooler` or `pgbouncer`, or the port is `6543`.
- The probe does not come back.

The warning is one of these two sentences:

`The listen connection host looks like a transaction pooler, which cannot keep a listen session. Set ListenConnectionString to a direct Postgres connection, or to a session pooler.`

`A probe wake-up did not come back on the listen connection. Set ListenConnectionString to a direct Postgres connection, or to a session pooler.`

After that warning, subscriptions read on `PollOnlyInterval` (default 100
milliseconds). A save in this process still wakes them. Events still
arrive. The wake-up is a hint. The events table is the record. A lost
wake-up delays the next read.

`PostgresNotificationQueueHealthCheck` reads how full the notification
queue is. The package page has the marks:
[Postgres event store](packages/eventsourcing-postgres.md).

## Related

- [Postgres partitions and tenants](POSTGRES_PARTITIONS.md)
- [Backend differences](BACKEND_DIFFERENCES.md)
- [Schema initialization](SCHEMA_INITIALIZATION.md)
- [Postgres event store](packages/eventsourcing-postgres.md)
- [Postgres projections](packages/projections-lightweight-postgres.md)
- [Postgres inbox](packages/messaging-postgres.md)
