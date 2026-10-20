# LawnDart.Projections.Lightweight.SqlServer

SQL Server `IViewStore` and `ICheckpointStore` for
`LawnDart.Projections.Lightweight`. Views and checkpoints survive a process
restart and are shared by every instance that uses the same tables.

Add this package when read models run on SQL Server. The projection host
stays in `LawnDart.Projections.Lightweight`.

## Registration

```csharp
using LawnDart.Projections.Lightweight.SqlServer;

services.AddSqlProjectionStores("default", connectionString);
```

Call `AddSqlProjectionStores` before `WithProjections`. When you omit the
schema, the SQL Server schema is the bounded context name. Pass
`schemaName: "dbo"` to keep the `dbo` schema.

After the host is built, create the tables once:

```csharp
await app.Services.InitializeSqlProjectionStoresAsync("default");
```

Run that from a connection that can create tables. After the tables exist,
a later call does not create them again.

## Types

`SqlViewStore` stores one row per projection instance in `ProjectionViews`.
`SqlCheckpointStore` stores one row per projection per node in
`ProjectionCheckpoints`. Both types are in the namespace
`LawnDart.Projections.Lightweight.SqlServer`.

## Related

- [Package map](README.md)
- [Lightweight projections](projections-lightweight.md)
- [Lightweight projections guide](../LIGHTWEIGHT_PROJECTIONS.md)
- [Backend selection](../BACKEND_SELECTION.md)
