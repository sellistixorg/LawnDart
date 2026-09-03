---
name: lawndart-event-store
description: Choose and register a LawnDart event store. UseInMemory for zero infra; UseSqlServer for durable SQL.
---

# Event store

v1 backends: **InMemory** and **SQL Server**.

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);

// Local / tests / Academy
services.AddBoundedContext("default").UseInMemory();

// Durable
services.AddBoundedContext("default")
    .UseSqlServer(o =>
    {
        o.ConnectionString = cs;
        o.RequireTenantId = false;
    });
```

Align `RequireTenantId` on `AddLawnDart` and `SqlServerEventStoreOptions`.

See `docs/BACKEND_SELECTION.md`.
