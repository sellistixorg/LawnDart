# Backend selection

LawnDart v1 ships two event-store backends.

| Backend | Best for | Infrastructure | Persistence |
|---|---|---|---|
| **InMemory** | Academy, unit tests, local spikes | None | Process lifetime |
| **SQL Server** | Durable apps, transactional outbox | SQL Server | Durable |

Both register through the same `AddBoundedContext` grammar. The verified swap is
**command dispatch, event persistence, aggregate reload, projection materialisation,
and read-back** — one application body, two registrations. Additional backends
implement `IEventLog` (the typed `IEventStore` session is shipped). A
third-party store must construct repositories with `EventSourcingRepositories`
and call `AddSnapshotWriteInfrastructure` (see [SNAPSHOTS.md](SNAPSHOTS.md));
the public repository constructors omit the write queue.

**Outbox and subscriptions are not covered by the swap guarantee.** Those paths
exist on both backends and have their own tests. Do not treat a green swap
contract as proof they behave identically.

The swap is not a single `Use*` line. Leaving InMemory also means:

- A connection string and `SqlServerEventStore.InitializeSchemaAsync` (SQL does
  not create tables on first append).
- `AddInMemoryProjectionStores` → `AddSqlProjectionStores` plus
  `InitializeSqlProjectionStoresAsync` if you materialise views.
- SQL snapshots are opt-in (`WithSnapshots` plus a strategy). `UseInMemory`
  registers the store automatically but still needs an
  `ISnapshotStrategyResolver` before anything is written.

## Portable store contract

`IEventStore` inherits `IStreamRegistry` — do not split. Changing a method
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
tagged `Category=Integration`. The InMemory → SQL swap contract lives under
`tests/LawnDart.Backends.Contract.Tests` (`Category=Contract`; the SQL leg is
also `Category=Integration`). That project also runs the event-log contract:
same payload on append/read, token and tag query without hydrate, and a host
that has not registered event CLR types can still read, filter, and copy
frames. Typed adapter tests sit on the same logs (unknown family and
content-type mismatch fail closed).

Local unit runs use `--filter Category!=Integration`. CI runs unit tests and
then `Category=Integration|Category=Contract` on the SQL Server, Lightweight,
and backend-contract test projects. SQL Server tests need Docker
(Testcontainers). In-process projection harnesses under `tests/.../Integration`
use the same trait so they stay out of the local unit job.

Academy optional profile — **local Windows path only**. Both launch profiles
use `Trusted_Connection=True` (integrated auth). CI does not run these
profiles; the contract test uses Testcontainers instead.

Console:

```bash
dotnet run --project demos/LawnDart.Demo.Academy --launch-profile SqlServer
```

WebApi (no source change — the `SqlServer` profile sets `EventStore__UseSqlServer=true`):

```bash
dotnet run --project demos/LawnDart.Demo.Academy.WebApi --launch-profile SqlServer
```

Required: a local SQL Server and `ConnectionStrings__Academy`, or
`LAWNDART_SQL_CONNECTION`. Default profile value:

`Server=localhost;Database=LawnDartAcademy;Trusted_Connection=True;TrustServerCertificate=True`

Create the database first. Academy does not call `InitializeSchemaAsync`; you
must create the event-store schema (or run the SQL tests' init against that
database) before the first command. Academy WebApi keeps
`AddInMemoryProjectionStores` even on the SQL profile — views stay
process-local. That is a named limitation, not a second event-store bug.

## Switching

Keep the bounded-context name. Swap only the `Use*` call. Repositories and
`IEventStore` stay keyed by that name. Bridge unkeyed services for `"default"`
when handlers resolve `IEventStore` without a key (Academy does this).
