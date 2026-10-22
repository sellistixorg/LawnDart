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

Create the tables from a deploy or admin step, once per deploy. Normal app
startup does not run it. See
[Schema initialization](../SCHEMA_INITIALIZATION.md).

```csharp
await provider.InitializePostgresProjectionStoresAsync("default");
```

Run that from a connection that can create tables. `MigrationConnectionString`
is that connection when the app role is not the owner. After the tables exist,
a later call does not create them again.

## Views and checkpoints

`PostgresViewStore` stores one jsonb row per projection instance in
`projection_views`. Spacing and key order can change. The decoded view is
the same. `PostgresCheckpointStore` stores one row per projection per node
in `projection_checkpoints`.

The runner writes the views and the checkpoint in one call when both are this
store. A failure leaves both as they were. A checkpoint save keeps the stored
position when the new position is older. Deleting a checkpoint clears the
position and raises the epoch. The row stays. A later read does not show a
checkpoint whose position is below zero. A save from a copy that started
with the old epoch is rejected.

View rows are jsonb. Spacing and key order can change. The decoded view is
the same. jsonb cannot store a null character (`\u0000`). That save throws.
A view that records a derived .NET type puts the type name in `$type`.
jsonb may move `$type` off the first property. `System.Text.Json` then
refuses the text unless you set `AllowOutOfOrderMetadataProperties` to true
on the options you pass to `JsonSerializer.Deserialize`:

```csharp
var options = new JsonSerializerOptions(ProjectionViewJson.Read)
{
    AllowOutOfOrderMetadataProperties = true
};
var view = JsonSerializer.Deserialize<MyView>(json, options);
```

## One copy per node number

`PostgresProjectionRunnerLock` is on unless you set `EnableSingleRunnerLock`
to false. It does not change how per-stream projections are split by stream
id, and it does not change the per-node global view id (`global:n0`,
`global:n1`, and so on). It only stops two copies that were both given the
same node number from doing that node's work.

Set `RunnerLockMode` to `Lease` for any transaction pooler. `Auto` is the
default, and it recognizes a pooler only by the host name: port 6543, or a
host containing `pooler` or `pgbouncer`. Any other host uses a session lock.
`SessionLock` holds a session lock. A transaction pooler cannot keep that
lock, so set `Lease` when the host name does not match. A wrong mode changes
when the waiting copy can take over.

The second copy waits and logs a warning that the two copies share that node
number. Give each copy its own node number.

On a direct connection, or a session pooler, the holder keeps a session lock
on `ListenConnectionString` (or on `ConnectionString` when that is the only
connection). If that connection drops, or the database restarts, the holder
does not save. It drops its in-memory state and tries to take the lead again.
It then reads the stored checkpoint and continues. The runner stops when the
host stops. A second copy that was waiting takes over while the first copy
waits. When the second copy stops, the first copy can take the lead again.

Each copy that takes the lead stores a number on the checkpoint row. That
number is the epoch. It starts at 1 and goes up each time a new copy takes
the lead. A checkpoint save is kept only when it carries the epoch this copy
started with, and the position is not older than the stored position. A save
from a copy that already lost the lead is rejected.

When `RunnerLockMode` is `Lease`, the store uses a lease row, renews it on a
timer, and logs that failover is slower: another copy can take over only
after the lease expires. `LeaseDuration` (default 15 seconds) is that wait.
`LeaseRenewInterval` (default 5 seconds) is how
often the holder extends it. `LockProbeInterval` (default 2 seconds) is how
often a session-lock holder checks that its connection is still up. You can
change each of those.

Point `ListenConnectionString` at Postgres directly, or at a session pooler,
when you want the waiting copy to take over as soon as the holder's
connection drops.

`RebuildAsync` clears the checkpoint and deletes the views in one transaction.
The checkpoint is cleared first: its position is set below zero and its epoch
goes up. Then the views are deleted. A copy that still has the old epoch
cannot save into that gap. A runner that finds no checkpoint, or a position
below zero, does not load stored views. It starts empty and replays.

The lock connection sends a keepalive after `LockKeepAliveSeconds` (default
30) of silence. It also asks Postgres to send TCP keepalives:
`LockTcpKeepAliveIdleSeconds` (default 30) is the wait before the first one,
`LockTcpKeepAliveIntervalSeconds` (default 10) is the gap after that, and
`LockTcpKeepAliveCount` (default 3) is how many can go unanswered before the
connection is dropped. A dropped connection releases the lock. The copy that
held it tries to take the lead again.

## Related

- [Package map](README.md)
- [Schema initialization](../SCHEMA_INITIALIZATION.md)
- [Lightweight projections](projections-lightweight.md)
- [Postgres event store](eventsourcing-postgres.md)
