# Backend selection

LawnDart v1 ships two event-store backends.

| Backend | Best for | Infrastructure | Persistence |
|---|---|---|---|
| **InMemory** | Academy, unit tests, local spikes | None | Process lifetime |
| **SQL Server** | Durable apps, transactional outbox | SQL Server | Durable |

Both register through the same `AddBoundedContext` grammar, so switching backends is a
one-line change. Additional backends can be added by implementing `IEventStore` and
extending `BoundedContextBuilder`.

## InMemory

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default").UseInMemory();
```

Use for:

- Academy (`dotnet run --project demos/LawnDart.Demo.Academy`)
- Given / when / then specs (`BddTestContext.CreateInMemory`)
- Local unit tests (`Category!=Integration`)

Data lasts for the process lifetime. Partition hashing can differ from
SQL Server, so treat routing as backend-specific.

## SQL Server

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .UseSqlServer(o =>
    {
        o.ConnectionString = connectionString;
        o.RequireTenantId = false;
    });
```

Use for production-shaped hosts, outbox, and Testcontainers integration tests
tagged `Category=Integration`.

Local unit runs use `--filter Category!=Integration`. CI runs unit tests and
then `Category=Integration`. SQL Server tests need Docker (Testcontainers).
In-process projection harnesses under `tests/.../Integration` use the same
trait so they stay out of the local unit job.

Academy optional profile:

```bash
dotnet run --project demos/LawnDart.Demo.Academy --launch-profile SqlServer
```

## Switching

Keep the bounded-context name. Swap only the `Use*` call. Repositories and
`IEventStore` stay keyed by that name. Bridge unkeyed services for `"default"`
when handlers resolve `IEventStore` without a key (Academy does this).
