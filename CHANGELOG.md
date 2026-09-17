# Changelog

All notable changes to LawnDart are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is below `1.0.0`, a **minor** bump may contain breaking
changes to the public API.

## [Unreleased]

## [0.4.0-alpha.6] — 2026-09-17

### Added

- `LawnDart.Analyzers` reports event schema versioning mistakes at `dotnet build`: two currents for one token (`LDT001`), a multi-type family with no `current: true` (`LDT002`), and an incomplete upcaster chain (`LDT003`). One-arg `[EventTypeName("token")]` is not a diagnostic. Warmup is still the runtime authority.

## [0.4.0-alpha.5] — 2026-09-16

### Fixed

- CI and release run `Category=Integration|Category=Contract` on the SQL Server, Lightweight, and backend-contract test projects instead of the whole solution, so a passing filtered run is not failed by assemblies with no matching tests.

## [0.4.0-alpha.4] — 2026-09-16

### Added

- `EventTypeCatalog.Materialize` builds a scoped immutable catalog. Typed resolve is family token plus `SchemaVersion`. `[EventTypeName(token, version: n, current: false)]` marks a historical or current schema. One-arg `[EventTypeName("token")]` is still version 1 and implicitly current when it is the only type for that family.
- [Event schema versioning](docs/EVENT_SCHEMA_VERSIONING.md) is the consumer guide: family tokens, first-class `SchemaVersion`, upcast on read, the fail-closed matrix, expand-contract deploy, and how to implement `IEventLog`.
- Typed reads throw dedicated `EventHydrationException` types when the stored version is newer than this process, the family is unknown, a historical type is missing, the content-type does not match, or the payload will not deserialize. Log and raw copy paths still return the frame. Lightweight projections back off five seconds on a stuck sequence; there is no skip override.
- `IEventUpcaster<TTo, TFrom>` and `WithUpcasters` register an upcast chain (v1 → v2 → v3). Warmup fails if any historical version cannot reach current. There is no downcast API.

### Changed

- `WithEventTypes` registers that catalog per bounded context. Two contexts do not share a mutable global catalog. `EventTypeNameResolver` remains a process-wide compatibility wrapper.
- Typed append rejects a historical CLR type for a family the catalog already knows. This process writes only its current type. The log still accepts frames from older binaries.
- Typed append stamps `SchemaVersion` on the log frame from the current CLR type and mirrors it onto `EventMetadata`. Hydrate reads the frame, not the metadata blob. Missing or zero version is `1`.
- Typed hydrate deserializes the stored version's CLR type, then upcasts to this process's current type. A missing hop throws `MissingEventUpcasterException`. The outbox publisher uses the same `EventSession` path. Log and raw copy still return the stored frame.
- The library `Book` aggregate throws `DomainException` for rule violations. Learning-path and testing docs assert `ThenThrows<DomainException>()`.
- Docs no longer mention the removed `IMessage` marker. Commands are `ICommand`; events are `IEvent`.

## [0.4.0-alpha.3] — 2026-09-16

### Added

- Third-party stores implement `IEventLog` (`AppendEvent` in, `RecordedEvent` out). Application code still uses `IEventStore`; that typed session is unchanged.
- The InMemory → SQL contract (`Category=Contract`) also covers the event log: the same payload on append/read, token and tag query without hydrate, and a host that has not registered event CLR types.
- `RawRecordedEvent` implements `IRawEvent` over a recorded frame. A host without the CLR event type appends and copies through `IEventLog`; typed hydrate still fails closed.
- [Why LawnDart](docs/WHY.md) contrasts LawnDart with Marten, Cratis, and Axon, and names the supported scope as a closed set (InMemory, SQL Server, in-process, `net10.0`).
- The library domain (`samples/Library.Domain`) is the canonical reference slice: a `Book` aggregate whose decision state (`BookState`) is not the catalog view (`BookCatalogView`), with three given/when/then specs in `samples/Library.Domain.Tests`.
- `build-kit/library-slice.json` is the canonical demo input. Skills excerpt host and domain snippets from `samples/Library.Domain` and `samples/Library.Host`. CI compiles the slice and runs its GWT tests on every change.
- `skills/BUILD_KIT.md` is the agent entry point for the in-repo build kit. It declares `Targets LawnDart 0.4`; CI fails if MinVer's major.minor is not that pair. Eventhesis is documented as one adapter over a generic slice spec, not as the kit's definition.

### Changed

- `IEventSerializer` now reads and writes `ReadOnlyMemory<byte>` (UTF-8 JSON by default). `EventSession` maps `IEvent` to `AppendEvent` and hydrates `RecordedEvent` so a store does not resolve CLR types.
- InMemory stores recorded events: append serializes, read hydrates a new instance. Mutating an appended event or its metadata is not visible on the next read.
- SQL Server stores recorded events: new payloads go to `EventPayload` (`VARBINARY`); old `EventData` (`NVARCHAR`) rows still read. `SchemaVersion` is a first-class column. The backend does not resolve CLR types.
- `IEventStore` is a typed adapter over `IEventLog`. InMemory and SQL Server implement the log; subscriptions hydrate in the adapter. A fail-closed hydrate does not advance the typed cursor.
- `IRawEvent` docs no longer name `OpaqueEvent` or `UnknownEvent` as LawnDart types. The catalog still rejects `IRawEvent`.
- SQL Server outbox rows copy the appended frame (family token, UTF-8 payload/metadata text, `SchemaVersion`, and `ContentType`) in the same transaction, instead of re-serializing a CLR event. Existing outbox rows without those columns read as version `1` and `application/json`. The message-transport publisher resolves types through the event catalog (token + schema version), not a private token-only map.
- Skills, Quickstart, and the package map state that InMemory serializes on append and that third-party stores implement `IEventLog`. `IEventSerializer` is documented as `ReadOnlyMemory<byte>`.
- The README, docs site landing, Start Here, and Overview lead with the same sentence: LawnDart is the .NET runtime that event-modeled systems compile into. The README quick start is a complete Counter — command, event, aggregate, `HandleCommandAsync`, and read-back — compiled in CI.
- Learning-path steps 2–4 teach the library `Book` domain. Snippets are excerpted from `samples/Library.Domain`. The README and Quickstart keep the short Counter.
- Snapshot docs state replace-in-place retention (one row per stream, no scavenge) and the strategy constructors from source. A dropped or bad snapshot costs replay time; the log stays the source of truth.
- The command–event–state matrix lives on [CES matrix](docs/CES_MATRIX.md) as the shapes an event model compiles into. Five cells are hosted; four are roadmap with no public type. Other pages link there instead of repeating the grid.
- Academy’s README points at the CES matrix instead of implying every From×To cell is hosted.

### Fixed

- `AddLawnDart` registers a default `AmbientTenantContextProvider` so `UseInMemory()` can resolve `IAggregateRepository` without a hand-written tenant registration. `RequireTenantId = false` hosts copy-paste as documented.

## [0.4.0-alpha.2] — 2026-09-14

### Added

- `UseInMemory()` registers an in-memory `ISnapshotStore` / `IDcbSnapshotStore` (replace-in-place) and an `IOutboxWriter`. In-memory hosts can persist snapshots and run the outbox processor without SQL Server.
- Third-party stores construct repositories through `EventSourcingRepositories` and register the write path with `AddSnapshotWriteInfrastructure`. The public constructors omit the snapshot write queue; without this seam a custom `Use*` host silently stops writing snapshots.
- A contract test runs the same application against InMemory and SQL Server (Testcontainers). The swap covers command dispatch, event persistence, aggregate reload, projection materialisation, and read-back. Outbox and subscriptions are not part of that guarantee.

### Changed

- Snapshot writes capture committed state on the calling thread, then enqueue wait-free onto a bounded channel (`DropOldest`). A hosted consumer performs the store I/O. A full channel drops the oldest pending write, increments a drop counter, and degrades `SnapshotWriteHealthCheck`. Store failures increment a failure metric and the same health check. `HandleCommandAsync` never waits on snapshot I/O and is not failed by a drop or a store error.

## [0.4.0-alpha.1] — 2026-09-14

### Added

- `WithCommandHandlers<TMarker>()` and `WithEventTypes<TMarker>()` register from the marker type's assembly. Prefer these over `Assembly.GetCallingAssembly()` fallbacks.

### Changed

- `AddLawnDartHttpCommands` no longer registers `ICommandHandler<T>`. Register handlers once with `WithCommandHandlers<TMarker>()` (or `WithCommandHandlers`) on the bounded context. The `"default"` context still resolves handlers unkeyed via an alias built through `ContextServiceProvider`. A scan that finds no handlers or event types fails at warmup and names the scanned assembly (and `Assembly.GetCallingAssembly()` when that fallback chose it).
- `AggregateSpec` assertion failures throw `BddSpecAssertionException` instead of `InvalidOperationException`. The new type does not derive from `InvalidOperationException`, so `Assert.ThrowsAsync<InvalidOperationException>(() => spec.RunAsync())` no longer passes when the spec itself failed. `ThenThrows<T>()` now matches derived exception types, not only an exact type.
- Messaging and authorization docs name only implementations shipped in these packages: `InMemoryMessageTransport` and `AddHttpAuthorizationContext`. Storage docs name `UseInMemory` and `UseSqlServer` only.

### Removed

- The four unhosted CES interfaces (`ICommandDelegator`, `IDownstreamActivity`, `IEventGenerator`, `IStateTransformer`). They had no host and no implementation. `IProjector<TState>` stays.
- `ProjectionDescriptor.UseRedis`. It was never read and defaulted to `true`.
- `MessagingOptions.ServiceBusConnectionString`. It was never read; these packages do not ship a Service Bus transport.

### Fixed

- Docs site links no longer point at the repo-root README (that file is not part of the DocFX build).

## [0.3.0-alpha.1] — 2026-09-10

### Added

- `EventMetadata` and `CommandMetadata` carry `TraceId` and `SpanId` (W3C hex).
- `AmbientMessageContext` publishes inbound `MessageContext` for metadata capture without changing `ICommandHandler<T>`.
- `MessageTrace` starts or continues a W3C Activity from `traceparent` and writes cheap `correlation_id` / `messaging.message_id` tags.

### Changed

- `CaptureCommandMetadata` reads `Activity.Current` and ambient `MessageContext` for correlation, causation, tenant, user, and W3C trace ids. Without a trace it still mints a correlation id.
- `ContextAwareCommandDispatcher` uses the inbound `MessageContext` (ambient publish + `traceparent` Activity) before `HandleAsync`.
- `HandleCommandAsync` sets envelope `CausationId` to the command id when the caller left it unset. Correlation is not copied from causation.
- HTTP command endpoints continue `traceparent`, publish ambient `MessageContext`, and may assign `ICommand.Id` from `Idempotency-Key` when the body omits it.
- HTTP authorization context uses the W3C trace id, not `HttpContext.TraceIdentifier`, as `CorrelationId`.
- Messaging hosts start Activity from inbound `traceparent` before handle. Outbox publish copies `TraceId` / `SpanId` / `traceparent` onto `MessageContext.Headers`. `CreateChild()` overlays the current span's trace headers.
- Event enrichment copies `IEvent.Timestamp` onto the envelope. It no longer overwrites business time with `DateTime.UtcNow`. `CommitTimestamp` is still set only at append. Time-travel (`toTimestamp`) uses envelope `Timestamp`.
- Event type names stored on append are the `[EventTypeName]` catalog token. Register types with `WithEventTypes`. CLR `FullName` is not written; older FullName rows still resolve as a read alias. Missing attributes and duplicate tokens fail at warmup.
- Sellistix.Patterns consumes these packages at `0.2.0-alpha.2` instead of shipping a second Core.

### Removed

- The empty `IMessage` marker. `ICommand` and `IEvent` no longer inherit it. `IMessageTransport` is unchanged.

## [0.2.0-alpha.2] — 2026-09-08

### Added

- Core ships `PublicAPI.Shipped.txt`. Adding or removing a public member fails the build unless that file is updated. Store implementers must keep the documented `IEventStore`, `IStreamRegistry`, and `IEventStoreSubscriptions` methods.
- `IAggregateRepository` can load and create aggregates by `string streamId`. Guid overloads still build `{type}:{id}` / `{tenant}:{type}:{id}`.
- Aggregates and DCB entities can declare closed `Handle(TCommand)` methods. `HandleCommandAsync` uses those when `HandleAsync<TCommand>` is not overridden. An unknown command throws.

### Changed

- Academy aggregates use closed `Handle(TCommand)`. Quickstart copy-paste is `Handle(CreateCounterCommand …)` and reads state back.
- Host grammar docs and skills state the frozen surface: `ICommandHandler<T>` for HTTP/jobs, closed `Handle(TCommand)` on aggregates, `string streamId` loads, `ProjectionBase` + `IMultiStreamEntityResolver`, and `UseInMemory` / `UseSqlServer`.
- `ICommandDispatcher` now ships in Core (same `LawnDart.Messaging` namespace). EventSourcing registers `ContextAwareCommandDispatcher` from `UseInMemory` / `UseSqlServer` / `WithCommandHandlers` and no longer needs the Messaging package for that registration.
- Aggregate and DCB store-internal mutators (`SetStreamId`, `SetVersion`, `SetCommittedVersion`, `SetTags`, `SetConsistencyTags`, `SetConsistencyMarker`, `ClearPendingEvents`) are no longer public. Load through `GetOrCreateAsync` / `HandleCommandAsync`.
- Packages install from nuget.org with `--prerelease` until a stable version exists.
- CI and release run `Category=Integration` (SQL Server Testcontainers) after unit tests.
- Docs mark the CES matrix: five hosted cells (✅) and four planned interfaces (🔧) with no host.
- Docs table the intentional verb differences (`Apply` vs `Emit`, `Handle` vs `HandleCommandAsync`, `IReactor` vs `IDcbReactor`). These names stay.
- The four planned CES cells (`ICommandDelegator`, `IDownstreamActivity`, `IEventGenerator`, `IStateTransformer`) are experimental (`LAWNDART002`). There is no host; implementing them does not register or run them.
- `IProjector<TState>` is experimental (`LAWNDART001`) and is not the authoring API. Author `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
- Projection docs: Lightweight authors `ProjectionBase`; `IProjector` is an optional unused stub. Eventhesis is described as slice JSON, not generated LawnDart types.

### Deprecated

- `HandleAsync<TCommand>` on aggregates and DCB entities. Declare `Handle(TCommand)` instead.

## [0.1.0-alpha.1] — 2026-09-07

### Removed

- Empty public type `LawnDartEventSourcingExtensions`. Registration stays on
  `AddBoundedContext(...).UseInMemory()` / `UseSqlServer()`.
- Unused `eventTypes` parameter on `BddTestContext.CreateInMemory`. The in-memory
  store does not need an event-type catalog.
- Unused `LawnDartOptions.AzureStorageConnection`. Nothing in the library read it.

### Added

- Monthly Dependabot updates for GitHub Actions, a `CODEOWNERS` file, and
  DocFX deploy to GitHub Pages on pushes to `main`. CodeQL is not enabled.
- `.gitattributes` normalizes text to LF in the repository and treats PNG
  and other image assets as binary.
- Quickstart and the first-aggregate learning-path step now run a command
  end-to-end and read the state back.
- Learning-path steps for DCB, reactions, and EDA testing include
  self-contained samples instead of pointers only.
- Package map at `docs/packages/README.md`, with a landing page per package.

### Fixed

- DocFX navbar uses a 64px transparent logo constrained to 32px instead of
  the 512px cream-backed mark, which overflowed the header.
- Doc samples for `IReactor.ReactAsync` and aggregate/DCB `HandleAsync` match
  the current signatures (`MessageContext`, `CancellationToken`).

### Changed

- Copyright and package author metadata name the legal entity as Sellistix LLC.
- `ContextAwareCommandDispatcher` docs now describe automatic registration
  via `UseInMemory` / `UseSqlServer` / `WithCommandHandlers`. There is no
  `AddContextAwareCommandDispatcher` method.
- Package versions are derived from git tags via MinVer. A tag of the form
  `v0.1.0-alpha.1` now produces packages at that version; untagged commits
  produce a deterministic prerelease suffix.
- The Academy console demo resolves `IEventStore`, `IAggregateRepository`, and
  `IDcbRepository` through the library's unkeyed `"default"` aliases instead of
  hand-written keyed bridges.

## [0.1.0-alpha] — 2026-08-20

### Added

- Happy-path packages: `LawnDart`, `LawnDart.EventSourcing`,
  `LawnDart.EventSourcing.SqlServer`, `LawnDart.Projections.Lightweight`,
  `LawnDart.AspNetCore`, `LawnDart.Authorization.AspNetCore`,
  `LawnDart.Messaging`, `LawnDart.Messaging.InMemory`, `LawnDart.Testing`.
- Academy console and WebApi demos on `UseInMemory()` (optional SQL profiles).
- Docs, learning path 1–8, Eventhesis contract, and `lawndart-*` agent skills.
- MIT license, SECURITY.md (`security@sellistix.com`), CI unit filter
  `Category!=Integration`.
