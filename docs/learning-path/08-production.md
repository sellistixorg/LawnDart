# Step 8 — Production shape

**Previous:** [Testing EDA](07-testing-eda.md)

Swap InMemory for SQL Server. Keep the same bounded-context name.

The verified swap is command dispatch, event persistence, aggregate reload,
projection materialisation, and read-back. **Outbox and subscriptions are not
covered by that guarantee** — they have their own tests and are not claimed
by "swap the registration."

You also change projection stores (`AddInMemoryProjectionStores` →
`AddSqlProjectionStores`) and run schema init (`InitializeSchemaAsync` /
`InitializeSqlProjectionStoresAsync`). SQL snapshots are opt-in
(`WithSnapshots`). Academy's `SqlServer` launch profile is the local Windows
path (`Trusted_Connection=True`); see [BACKEND_SELECTION.md](../BACKEND_SELECTION.md).

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs) via `WithCommandHandlers<TMarker>()` then `AddLawnDartHttpCommands` / `MapLawnDartCommands`.
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization.
3. **Load** by `string streamId` when the stream is not `{type}:{guid}`.
4. **Projections:** `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** `AddBoundedContext(name).UseInMemory()` / `UseSqlServer(...)`.

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddSqlProjectionStores("default", cs);
services.AddBoundedContext("default")
    .UseSqlServer(o =>
    {
        o.ConnectionString = cs;
        o.RequireTenantId = false;
        o.EnableOutbox = true;
    })
    .WithCommandHandlers<RegisterStudentCommandHandler>()
    .WithProjections([typeof(StudentSummaryProjection).Assembly]);

services.AddLawnDartHttpCommands(typeof(RegisterStudentCommand).Assembly);
services.AddHttpAuthorizationContext();
app.MapLawnDartCommands();
app.MapProjectionQueries("default");
```

Checklist:

- [BACKEND_SELECTION.md](../BACKEND_SELECTION.md) — InMemory vs SQL
- [OUTBOX_PATTERN.md](../OUTBOX_PATTERN.md) — durable publish
- [AUTHORIZATION.md](../AUTHORIZATION.md) — HTTP claims
- [HTTP_COMMANDS.md](../HTTP_COMMANDS.md) — POST mapping
