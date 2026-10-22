# LawnDart.Projections.Lightweight.Postgres

Postgres view store, checkpoint store, and single-runner lock for `LawnDart.Projections.Lightweight`.

## What this project provides

- `PostgresViewStore` (`IViewStore`). View rows are jsonb.
- `PostgresCheckpointStore` (`ICheckpointStore` and `IAtomicProjectionFlush`)
- `PostgresProjectionRunnerLock` (`IProjectionRunnerLock`)
- `AddPostgresProjectionStores` and `InitializePostgresProjectionStoresAsync`

```csharp
using LawnDart.Projections.Lightweight.Postgres;

services.AddPostgresProjectionStores("default", connectionString);
```

After the host is built, create the tables once:

```csharp
await app.Services.InitializePostgresProjectionStoresAsync("default");
```

The `default` context uses schema `lawndart`. Any other context uses its own name.
Pass `schemaName` to choose a different schema. The `public` schema is rejected.

The single-runner lock is on. Set `EnableSingleRunnerLock` to false to let every copy run.
Set `RunnerLockMode` to `Lease` for any transaction pooler. `Auto` recognizes a pooler
only by host name: port 6543, or a host containing `pooler` or `pgbouncer`.
Point `ListenConnectionString` at Postgres directly, or at a session pooler, when a
transaction pooler sits in front of `ConnectionString`.

See [the package page](https://github.com/sellistixorg/LawnDart/blob/main/docs/packages/projections-lightweight-postgres.md).
