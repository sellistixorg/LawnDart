# Snapshots

LawnDart snapshots are a cache of derived state. The event log is the source
of truth. A missing, dropped, or corrupt snapshot costs replay time.

## Retention

Snapshots are **replace-in-place**. One row per stream (`StreamId`) or DCB
identity (`DcbId`). A write upserts that row. Newest wins.

SQL Server: `EventSnapshots` is keyed on `StreamId`; `DcbSnapshots` is keyed
on `DcbId`. Save is `UPDATE` then `INSERT` if no row. In-memory:
`InMemorySnapshotStore` replaces the dictionary entry for that key.

Because there is only one generation, a torn or corrupt row overwrites the last
good snapshot. Load then returns empty and the repository **falls back to full
replay**. The log stays correct. Load time is what you pay. That is why
the write path captures state on the calling thread at the committed version
(the durability contract below) instead of serializing a live aggregate later.

## Durability contract

1. State is captured **synchronously** at the committed version. The snapshot
   can never represent a version that did not exist.
2. Enqueue is **wait-free**. A successful `HandleCommandAsync` never waits on
   snapshot IO, saturated or not.
3. A full channel is a **recorded, non-fatal event**: drop counter plus a
   degraded health check. It does not block and does not throw to the
   caller.
4. Sustained drops mean the store cannot keep up. That is an operational
   signal. The fallback is full replay.

`SnapshotWriteHealthCheck` reports **degraded** when the write channel has
dropped pending work or the store has failed. Register it with the host's
health checks if you want that signal on a probe.

`UseInMemory()` registers `InMemorySnapshotStore` (replace-in-place, one entry
per stream / DCB id). SQL Server hosts opt in with `WithSnapshots`. Neither
backend writes until an `ISnapshotStrategyResolver` says so (default is
`NeverSnapshotStrategy`).

A custom `Use*` must call `AddSnapshotWriteInfrastructure` and construct
repositories with `EventSourcingRepositories.CreateAggregateRepository` /
`CreateDcbRepository`. The public repository constructors omit the write
queue and skip every snapshot write.

## Strategy defaults

From source, not convention:

| Type | When it writes | Constructor |
|---|---|---|
| `NeverSnapshotStrategy` | Never. System-wide default for any type without a registration. | `Instance` singleton |
| `EventCountSnapshotStrategy` | `EventsSinceLastSnapshot >= threshold` | `int threshold` (must be `> 0`). No default threshold. |
| `DynamicSnapshotStrategy` | Event-count leg or time leg, whichever fires first. The time leg runs only when `LastSnapshotUtc` is set, so the first append does not snapshot immediately. | `int eventThreshold` (`> 0`) and `TimeSpan timeThreshold` (positive). No default pair. |

Unregistered types keep `NeverSnapshotStrategy`. Opt in with `WithSnapshots`:

```csharp
.WithSnapshots(config =>
{
    config.RegisterForAggregate<Book>(new EventCountSnapshotStrategy(500));
    config.RegisterForDcb<EnrollmentEntity>(new EventCountSnapshotStrategy(500));
});
```

`500` in that example is from `SnapshotStrategyResolver`'s remarks, not a
baked-in default. The strategy types note that upstream benchmarks
(not in this repository) put the load-vs-replay crossover around 400 to 1,000
events for typical state sizes. A threshold below that range can make loads
slower. Measure against your state type before enabling writes.
