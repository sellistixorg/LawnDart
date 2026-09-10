# Changelog

All notable changes to LawnDart are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is below `1.0.0`, a **minor** bump may contain breaking
changes to the public API.

## [Unreleased]

### Added

- `EventMetadata` and `CommandMetadata` carry `TraceId` and `SpanId` (W3C hex).

### Changed

- Event enrichment copies `IEvent.Timestamp` onto the envelope. It no longer overwrites business time with `DateTime.UtcNow`. `CommitTimestamp` is still set only at append. Time-travel (`toTimestamp`) uses envelope `Timestamp`.
- Event type names stored on append are the `[EventTypeName]` catalog token. CLR `FullName` is not written; older FullName rows still resolve as a read alias.
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
