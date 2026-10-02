# Step 8. Production shape

**Previous:** [Testing EDA](07-testing-eda.md)

Swap InMemory for SQL Server. Keep the same bounded-context name.

The verified swap is command dispatch, event persistence, aggregate reload,
projection materialisation, and read-back. Outbox and subscriptions have
their own tests. A green swap does not prove those paths.

You also change projection stores (`AddInMemoryProjectionStores` to
`AddSqlProjectionStores`) and run schema init (`InitializeSchemaAsync` /
`InitializeSqlProjectionStoresAsync`). SQL snapshots are opt-in
(`WithSnapshots`). Academy's `SqlServer` launch profile is the local Windows
path (`Trusted_Connection=True`); see [BACKEND_SELECTION.md](../BACKEND_SELECTION.md).

Host grammar: [DI Grammar](../DI_GRAMMAR.md).

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
    .WithEventTypes<StudentRegistered>()
    .WithProjections([typeof(StudentSummaryProjection).Assembly]);

services.AddLawnDartHttpCommands(typeof(RegisterStudentCommand).Assembly);
services.AddHttpAuthorizationContext();
app.MapLawnDartCommands();
app.MapProjectionQueries("default");
```

## Schema deploy

Additive JSON rolls freely: new named properties on the current type do
not require a `SchemaVersion` bump. A breaking change (renamed meaning, a
removed required field, or a remapped `[PropertyOrder]` number) is
expand-contract. Ship readers that understand `SchemaVersion` N+1 before
any process writes N+1. Old binaries fail closed on those newer rows
(`EventSchemaTooNewException`) and may keep appending the version that
was current for them. There is no remote downcaster and no skip override.

See [Event schema versioning](../EVENT_SCHEMA_VERSIONING.md). Envelope
fields: [Metadata](../METADATA.md).

## Wipe, then recreate

Drop and recreate SQL event and outbox tables, then run
`InitializeSchemaAsync`. Opening an older table throws and names
drop-and-recreate. There is no in-place `ALTER` and no migration tool.

Checklist:

- [BACKEND_SELECTION.md](../BACKEND_SELECTION.md): InMemory vs SQL
- [OUTBOX_PATTERN.md](../OUTBOX_PATTERN.md): durable publish
- [AUTHORIZATION.md](../AUTHORIZATION.md): HTTP claims
- [HTTP_COMMANDS.md](../HTTP_COMMANDS.md): POST mapping
- [EVENT_SCHEMA_VERSIONING.md](../EVENT_SCHEMA_VERSIONING.md): family tokens, upcast, expand-contract
