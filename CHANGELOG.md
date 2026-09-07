# Changelog

All notable changes to LawnDart are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is below `1.0.0`, a **minor** bump may contain breaking
changes to the public API.

## [Unreleased]

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
