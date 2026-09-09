# Backend selection

LawnDart v1 ships two event-store backends.

| Backend | Best for | Infrastructure | Persistence |
|---|---|---|---|
| **InMemory** | Academy, unit tests, local spikes | None | Process lifetime |
| **SQL Server** | Durable apps, transactional outbox | SQL Server | Durable |

Both register through the same `AddBoundedContext` grammar, so switching backends is a
one-line change. Additional backends can be added by implementing `IEventStore` and
extending `BoundedContextBuilder`.

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
