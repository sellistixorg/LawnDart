# Backend selection

LawnDart ships three event-store backends.

| Backend | Best for | Infrastructure | Persistence |
|---|---|---|---|
| **InMemory** | Academy, unit tests, local spikes | None | Process lifetime |
| **SQL Server** | Durable apps, transactional outbox | SQL Server | Durable |
| **Postgres** | Durable apps on Postgres 15 or newer | Postgres | Durable |

Both register through the same `AddBoundedContext` grammar. The verified swap is
**command dispatch, event persistence, aggregate reload, projection materialisation,
and read-back**: one application body, two registrations. Additional backends
implement `IEventLog` (the typed `IEventStore` session is shipped). A
third-party store must construct repositories with `EventSourcingRepositories`
and call `AddSnapshotWriteInfrastructure` (see [SNAPSHOTS.md](SNAPSHOTS.md));
the public repository constructors omit the write queue.

Outbox and subscriptions exist on both backends and have their own tests. A
green swap contract does not prove those paths behave identically.

Leaving InMemory also means:

- A connection string and `SqlServerEventStore.InitializeSchemaAsync` (SQL does
  not create tables on first append).
- `AddInMemoryProjectionStores` to `AddSqlProjectionStores` plus
  `InitializeSqlProjectionStoresAsync` if you materialise views. Both SQL Server
  methods are in `LawnDart.Projections.Lightweight.SqlServer`. Add
  `using LawnDart.Projections.Lightweight.SqlServer`.
- `AddSqlInboxStore` plus `InitializeSqlInboxStoreAsync` when reactor or
  processor deduplication must survive a restart. The in-memory inbox is
  per process.
- A transport for the other process: `AddRabbitMqTransport`,
  `AddAzureServiceBusTransport`, or `AddAwsSqsTransport`. A single process keeps
  `AddInMemoryMessaging`. See [MESSAGE_TRANSPORTS.md](MESSAGE_TRANSPORTS.md).
- SQL snapshots are opt-in (`WithSnapshots` plus a strategy). `UseInMemory`
  registers the store automatically but still needs an
  `ISnapshotStrategyResolver` before anything is written.

## Portable store contract

`IEventStore` inherits `IStreamRegistry`. Changing a method
signature here breaks InMemory, SQL Server, and any third-party store.
`IEventStoreSubscriptions` is optional for a custom store; both shipped
backends implement it.

**`IEventStore`**

| Method | Role |
|---|---|
| `ReadStreamAsync(streamId, fromVersion = 0, toVersion = null, toTimestamp = null, ct)` | Stream read (list) |
| `ReadStreamEnumerableAsync(streamId, fromVersion = 0, toVersion = null, toTimestamp = null, ct)` | Stream read (async sequence) |
| `ReadByQueryAsync(query, fromSequencePosition = null, limit = null, toSequencePosition = null, toTimestamp = null, ct)` | DCB query (list + consistency marker) |
| `ReadByQueryStreamAsync(query, fromSequencePosition = null, toSequencePosition = null, toTimestamp = null, ct)` | DCB query (async sequence) |
| `AppendAsync(streamId, events, expectedVersion = null, metadata = null, tags = null, ct)` | Stream append |
| `AppendAsync(events, condition, metadata = null, tags = null, ct)` | DCB append |
| `GetCurrentSequenceAsync(ct)` | Store-wide head |
| `GetMaxSequencePositionAsync(query, fromSequencePosition = null, toSequencePosition = null, toTimestamp = null, ct)` | Filtered max sequence |

**`IStreamRegistry`** (inherited)

| Method | Role |
|---|---|
| `GetStreamAsync(streamId, ct)` | One stream's metadata |
| `GetStreamsByAggregateTypeAsync(aggregateType, ct)` | Discover by type |
| `GetStreamsByTagAsync(tag, ct)` | Discover by tag |
| `EnumerateStreamIdsAsync(prefix = null, ct)` | List stream IDs |
| `GetStreamsUpdatedAfterAsync(afterSequencePosition, limit = null, ct)` | Incremental catch-up |
| `GetStreamCountAsync(prefix = null, ct)` | Count |

**`IEventStoreSubscriptions`**

| Method | Role |
|---|---|
| `Subscribe(subscriberId, fromSequence, filter = null, ct)` | Catch-up → live push |

Core freezes this surface in `PublicAPI.Shipped.txt`. Adding or removing a
public Core member without editing that file fails the build.

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
tagged `Category=Integration`. The shared backend contract lives under
`tests/LawnDart.Backends.Contract.Tests` (`Category=Contract`; the SQL leg is
also `Category=Integration`). One container serves every SQL Server contract
test in a run. Each test uses its own stream, tag, or schema. In-memory runs
the same tests when it can. A test that needs a hook the backend does not
have is skipped with a reason. The areas are the event log, event store reads
and appends, dynamic consistency boundary, tags, stream registry,
subscriptions, read horizon, snapshots, outbox, inbox, checkpoint store, and
view store. The swap test still covers dispatch, persist, reload, project,
and read-back.

Local unit runs use `--filter Category!=Integration`. That includes the
in-memory contract tests. CI runs unit tests and then
`Category=Integration|Category=Contract` on the SQL Server, Lightweight,
and backend-contract test projects. SQL Server tests need Docker
(Testcontainers). In-process projection harnesses under `tests/.../Integration`
use the same trait so they stay out of the local unit job.

Academy optional profile is a **local Windows path only**. Both launch profiles
use `Trusted_Connection=True` (integrated auth). CI does not run these
profiles; the contract test uses Testcontainers instead.

Console:

```bash
dotnet run --project demos/LawnDart.Demo.Academy --launch-profile SqlServer
```

WebApi (the `SqlServer` profile sets `EventStore__UseSqlServer=true`):

```bash
dotnet run --project demos/LawnDart.Demo.Academy.WebApi --launch-profile SqlServer
```

Required: a local SQL Server and `ConnectionStrings__Academy`, or
`LAWNDART_SQL_CONNECTION`. Default profile value:

`Server=localhost;Database=LawnDartAcademy;Trusted_Connection=True;TrustServerCertificate=True`

Create the database first. The host creates event-store and outbox tables at
startup. Both SQL profiles set `EnableOutbox` and register the in-memory
outbox publisher. Academy WebApi also creates view and checkpoint tables and
reads those views from SQL Server. Academy console keeps hand-written
projectors, so that profile does not register Lightweight projection stores.

## Postgres

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .UsePostgres(o =>
    {
        o.ConnectionString = connectionString;
        o.RequireTenantId = false;
    });
```

`UsePostgres` is in `LawnDart.EventSourcing.Postgres`. Call
`PostgresEventStore.InitializeSchemaAsync` before the first append. The
`default` context stores tables in schema `lawndart`. Any other context
uses its own name as the schema. Set `SchemaName` to choose a different one.
Tables do not go in `public`.

This release is the event store: append, read, tags, the dynamic consistency
boundary, the stream registry, and the read horizon. `EnableOutbox = true`
is rejected when the store is built. Leave `EnableOutbox` false until the
outbox release. Snapshot
tables, the inbox, and projection views are a later release. A same-process
reader wakes when a save in that process commits. A notification with the
newest position is sent on the append. The listener that reads that
notification from another process is a later release.

Postgres 15 or newer. Event ids are unique per tenant. Stream ids match
exactly, including case. JSON payloads are stored as jsonb, so spacing and
key order can change. The decoded event is the same. A JSON payload that
contains a null character is stored as bytes and comes back unchanged.

The shared contract suite runs on Postgres in
`tests/LawnDart.Backends.Contract.Tests`. One Postgres container serves the
run. Tests that need the outbox, inbox, snapshots, projection stores, or the
live subscription suite are skipped with a reason.

## Switching

Keep the bounded-context name. Swap only the `Use*` call. Repositories and
`IEventStore` stay keyed by that name. Bridge unkeyed services for `"default"`
when handlers resolve `IEventStore` without a key (Academy does this).
