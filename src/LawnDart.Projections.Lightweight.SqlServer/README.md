# LawnDart.Projections.Lightweight.SqlServer

SQL Server view store and checkpoint store for `LawnDart.Projections.Lightweight`.

## What this project provides

- `SqlViewStore` (`IViewStore`)
- `SqlCheckpointStore` (`ICheckpointStore`)
- `AddSqlProjectionStores` and `InitializeSqlProjectionStoresAsync`

```csharp
using LawnDart.Projections.Lightweight.SqlServer;

services.AddSqlProjectionStores("default", connectionString);
```

After the host is built, create the tables once:

```csharp
await app.Services.InitializeSqlProjectionStoresAsync("default");
```

When you omit the schema, the SQL Server schema is the bounded context name.
Pass `schemaName: "dbo"` to keep the `dbo` schema.
