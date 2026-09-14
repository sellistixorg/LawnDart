# Snapshots

LawnDart snapshots are a **cache of derived state**. The event log is the source
of truth. A missing, dropped, or corrupt snapshot costs replay time and nothing
else.

<!-- TODO(RDY-11) replace-in-place, scavenging, strategy defaults -->

## Durability contract

1. State is captured **synchronously** at the committed version. The snapshot
   can never represent a version that did not exist.
2. Enqueue is **wait-free**. A successful `HandleCommandAsync` never waits on
   snapshot IO, saturated or not.
3. A full channel is a **recorded, non-fatal event** — drop counter plus a
   degraded health check — never a silent block and never an exception to the
   caller.
4. Sustained drops mean the store cannot keep up. That is an operational signal,
   surfaced as one, and the fallback is full replay.

`SnapshotWriteHealthCheck` reports **degraded** when the write channel has
dropped pending work or the store has failed. Register it with the host's
health checks if you want that signal on a probe.

`UseInMemory()` registers `InMemorySnapshotStore` (replace-in-place, one entry
per stream / DCB id). SQL Server hosts opt in with `WithSnapshots`. Neither
backend writes until an `ISnapshotStrategyResolver` says so (default is
`NeverSnapshotStrategy`).

Third-party stores (`UseBoomerang`, a custom `Use*`) must not `new
AggregateRepository` / `new DcbRepository` with the public constructors —
those omit the write queue and skip every snapshot write. Call
`AddSnapshotWriteInfrastructure` from the `Use*` method and construct
repositories with `EventSourcingRepositories.CreateAggregateRepository` /
`CreateDcbRepository`.
