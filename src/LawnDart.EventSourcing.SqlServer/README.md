# LawnDart.EventSourcing.SqlServer

SQL Server persistence provider for `LawnDart.EventSourcing`. InMemory remains
the zero-infra default.

## What's included

- **`SqlServerEventStore`**: `IEventStore` + portable subscriptions
- **`SqlServerOutboxWriter`**: transactional outbox with event appends
- **`UseSqlServer(...)`** on `BoundedContextBuilder`
- **`SqlServerSnapshotStore`**: opt-in via `.WithSnapshots()`

## Installation

```bash
dotnet add package LawnDart.EventSourcing.SqlServer --prerelease
```

## Getting started

```csharp
services.AddLawnDart();
services.AddBoundedContext("default")
    .UseSqlServer(options =>
    {
        options.ConnectionString = "Server=.;Database=MyApp;Trusted_Connection=True;";
    });
```

Call `InitializeSchemaAsync` from a deploy or admin step. Normal app startup
calls `VerifySchemaAsync` instead. That call only reads. See
[Schema initialization](https://github.com/sellistixorg/LawnDart/blob/main/docs/SCHEMA_INITIALIZATION.md).
