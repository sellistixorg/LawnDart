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

Create the tables from a deploy or admin step, once per deploy:

```csharp
await provider.InitializeSqlProjectionStoresAsync("default");
```

See [Schema initialization](https://github.com/sellistixorg/LawnDart/blob/main/docs/SCHEMA_INITIALIZATION.md).

When you omit the schema, the SQL Server schema is the bounded context name.
Pass `schemaName: "dbo"` to keep the `dbo` schema.
