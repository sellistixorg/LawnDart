# Step 8 — Production shape

**Previous:** [Testing EDA](07-testing-eda.md)

Swap InMemory for SQL Server. Keep the same bounded-context name.

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
