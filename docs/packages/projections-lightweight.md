# LawnDart.Projections.Lightweight

In-process projection host. Includes the in-memory view store, checkpoint
store, SDK, and partitioning types.

SQL Server view and checkpoint stores are in
[`LawnDart.Projections.Lightweight.SqlServer`](projections-lightweight-sqlserver.md).
Postgres view and checkpoint stores are in
[`LawnDart.Projections.Lightweight.Postgres`](projections-lightweight-postgres.md).

Take it when you need queryable read models. LawnDart ships this projection
engine only. Author `ProjectionBase<TView>` plus attributes. Multi-stream
views implement `IMultiStreamEntityResolver`.

Register the view stores **before** `WithProjections`.

## Registration

```csharp
services.AddInMemoryProjectionStores("default");
ctx.WithProjections([typeof(MyProjection).Assembly]);
app.MapProjectionQueries("default");
```

For SQL Server, add `LawnDart.Projections.Lightweight.SqlServer` and call
`AddSqlProjectionStores("default", connectionString)`. Add
`using LawnDart.Projections.Lightweight.SqlServer`. Keep the same context
name.

For Postgres, add `LawnDart.Projections.Lightweight.Postgres` and call
`AddPostgresProjectionStores("default", connectionString)`. Add
`using LawnDart.Projections.Lightweight.Postgres`.

## One transaction, and one copy per node number

`IAtomicProjectionFlush` is optional. When the checkpoint store or the view
store implements it, the runner saves every view in the flush and the
checkpoint in one transaction. A failure leaves both as they were. A store
that does not implement it keeps two calls: views, then the checkpoint.
SQL Server keeps the two calls.

`IProjectionRunnerLock` is optional. With no lock, every copy runs. The lock
does not change how per-stream projections are split by stream id, and it
does not change the per-node global view id (`global:n0`, `global:n1`, and
so on). It only stops two copies that were both given the same node number
from doing that node's work. Postgres registers a lock unless
`EnableSingleRunnerLock` is false. The Postgres package page explains the
epoch, the waiting copy, and the lease used when the connection cannot hold
a session lock.

Academy WebApi maps GET `/api/views/students/{studentId}` this way.

## Related

- [Package map](README.md)
- [Lightweight projections](../LIGHTWEIGHT_PROJECTIONS.md)
- [Reading state](../learning-path/04-reading-state.md)
