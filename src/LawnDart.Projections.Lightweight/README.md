# LawnDart.Projections.Lightweight

In-process projection host. View and checkpoint stores in this package are in-memory.

```csharp
ctx.UseInMemory();
ctx.WithProjections(...);
```

SQL Server view and checkpoint stores are in `LawnDart.Projections.Lightweight.SqlServer`.
Postgres view and checkpoint stores are in `LawnDart.Projections.Lightweight.Postgres`.
`IAtomicProjectionFlush` saves views and the checkpoint in one transaction when the store offers it.
`IProjectionRunnerLock` keeps one running copy per node number when a store registers it. With no lock, every copy runs.
