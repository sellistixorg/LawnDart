# LawnDart.Projections.Lightweight.Postgres

Postgres `IViewStore` and `ICheckpointStore` for
`LawnDart.Projections.Lightweight`. Views and checkpoints survive a process
restart and are shared by every instance that uses the same tables.

Add this package when read models run on Postgres. The projection host
stays in `LawnDart.Projections.Lightweight`.

## Registration

```csharp
using LawnDart.Projections.Lightweight.Postgres;

services.AddPostgresProjectionStores("default", connectionString, configure: o =>
{
    o.ListenConnectionString = listenConnectionString;
});
```

Call `AddPostgresProjectionStores` before `WithProjections`. When you omit the
schema, the `default` context uses schema `lawndart` and any other context
uses its own name. Pass `schemaName` to choose a different schema. The
`public` schema is rejected.

After the host is built, create the tables once:

```csharp
await app.Services.InitializePostgresProjectionStoresAsync("default");
```

Run that from a connection that can create tables. `MigrationConnectionString`
is that connection when the app role is not the owner. After the tables exist,
a later call does not create them again.

## Views and checkpoints

`PostgresViewStore` stores one jsonb row per projection instance in
`projection_views`. Spacing and key order can change. The decoded view is
the same. `PostgresCheckpointStore` stores one row per projection per node
in `projection_checkpoints`.

The runner writes the views and the checkpoint in one call. A failure leaves
both as they were. A checkpoint save keeps the stored position when the new
position is older.

## One copy per node number

`PostgresProjectionRunnerLock` is on unless you set `EnableSingleRunnerLock`
to false. It does not change how per-stream projections are split by stream
id, and it does not change the per-node global view id (`global:n0`,
`global:n1`, and so on). It only stops two copies that were both given the
same node number from doing that node's work.

The second copy waits and logs a warning that the two copies share that node
number. Give each copy its own node number.

On a direct connection, or a session pooler, the holder keeps a session lock
on `ListenConnectionString` (or on `ConnectionString` when that is the only
connection). If that connection drops, or the database restarts, the holder
stops before its next save and the waiting copy takes over.

Each copy that takes the lead stores a number on the checkpoint row. That
number is the epoch. It starts at 1 and goes up each time a new copy takes
the lead. A checkpoint save is kept only when it carries the epoch this copy
started with, and the position is not older than the stored position. A save
from a copy that already lost the lead is rejected.

A transaction pooler cannot keep that session lock. The store then uses a
lease row, renews it on a timer, and logs that failover is slower: another
copy can take over only after the lease expires. `LeaseDuration` (default
15 seconds) is that wait. `LeaseRenewInterval` (default 5 seconds) is how
often the holder extends it. `LockProbeInterval` (default 2 seconds) is how
often a session-lock holder checks that its connection is still up. You can
change each of those.

Point `ListenConnectionString` at Postgres directly, or at a session pooler,
when you want the waiting copy to take over as soon as the holder's
connection drops.

## Related

- [Package map](README.md)
- [Lightweight projections](projections-lightweight.md)
- [Postgres event store](eventsourcing-postgres.md)
