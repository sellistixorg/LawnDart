# Contributing to LawnDart

Thank you for your interest in contributing. This guide covers scope,
development setup, project conventions, and the definition of done.

## Scope

LawnDart is pre-1.0 and the public API is still settling. To avoid wasted
effort, please open an issue before starting on any of the following:

- **A new event store backend.** InMemory and SQL Server are the two supported
  stores. Others are not currently planned.
- **A new message transport.** `LawnDart.Messaging.InMemory` is the only
  transport today.
- **Changes that reshape public API surface** — new abstractions, renamed
  members, or altered interfaces.

Bug fixes, tests, documentation, and additions behind the existing abstractions
are always welcome without prior discussion.

## Development setup

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) or later
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — required
  for SQL Server integration tests (Testcontainers)
- PowerShell 7+ or bash

### First build

```bash
git clone https://github.com/sellistixorg/LawnDart.git
cd LawnDart

dotnet restore
dotnet build
```

### Running tests

```bash
# Unit tests only (no Docker)
dotnet test --filter "Category!=Integration"

# All tests (unit + SQL Server integration — requires Docker)
dotnet test
```

### Running the Academy demo

```bash
# Zero infrastructure — InMemory backend, .NET 10 only
dotnet run --project demos/LawnDart.Demo.Academy
```

## Branch strategy

| Branch | Purpose |
|---|---|
| `main` | Stable, releasable. All PRs target `main`. |
| `feature/<name>` | New features or pattern implementations |
| `fix/<name>` | Bug fixes |
| `docs/<name>` | Documentation-only changes |

### PR conventions

- PRs must pass CI (build + unit tests + DocFX) before merging.
- Each PR should address a single concern.
- PR description must include **what** changed, **why**, and any **trade-offs**.
- Commit subjects: imperative mood, 72 characters or less, no trailing period
  — `Add snapshot strategy resolver`, not `Added...`. Use the commit body for
  **why**, not what.

## Developer Certificate of Origin

Contributions are accepted under the project's [MIT license](LICENSE). To
confirm you have the right to submit your work, sign off every commit:

```bash
git commit -s -m "Add snapshot strategy resolver"
```

That appends a `Signed-off-by` line from your `git config user.name` and
`user.email`, certifying that you agree to the
[Developer Certificate of Origin](https://developercertificate.org/). There is
no separate CLA to sign.

If you forget, add the sign-off retroactively and force-push the branch:

```bash
git commit --amend -s --no-edit   # most recent commit only
git rebase --signoff HEAD~3       # last three commits
git push --force-with-lease
```

### AI-assisted contributions

AI coding assistants are permitted. The sign-off means the same thing when you
use one: it is still **your** certification, and it asserts that you have read
the code, understand why each line is there, and have no reason to believe it
was copied from a source whose license is incompatible with MIT. If you cannot
explain a design decision during review, do not submit it.

Enable your assistant's public-code filtering where it offers one. Large
generated diffs that the author cannot walk through will be closed.

## Definition of done

| Checklist | Requirement |
|---|---|
| Build | `dotnet build` passes |
| Tests | Unit tests cover the new code. SQL integration tests use `Category=Integration` |
| XML docs | Public and protected members have `<summary>` documentation |
| Demo | User-facing features are reachable from Academy when relevant |
| Docs | A `docs/` guide is created or updated |
| Changelog | A line is added under `## [Unreleased]` in `CHANGELOG.md` |

## Coding standards

- Nullable reference types enabled (`<Nullable>enable</Nullable>`).
- Records for commands and events.
- `async`/`await` throughout — never `.Result` or `.Wait()` in production code.
- File-scoped namespaces.
- xUnit for all tests. Use `WaitForAsync` for eventual-consistency assertions.
- Runtime dependencies are limited to `Microsoft.Extensions.*`,
  `Microsoft.AspNetCore.*`, `Microsoft.Data.SqlClient`, `OpenTelemetry`, and
  `MemoryPack`. Open an issue before adding any new third-party runtime
  dependency, and add the version to `Directory.Packages.props` rather than the
  individual `.csproj`.

## Project layout

When adding a new source package:

1. Create the project under `src/LawnDart.<Name>/` (Core is `src/LawnDart`).
2. Add it to `LawnDart.sln`.
3. Use `AddLawnDart` / `AddBoundedContext` DI grammar.
4. Add a corresponding test project under `tests/LawnDart.<Name>.Tests/`.
5. Update `CHANGELOG.md` and the package map in `README.md`.

## Getting help

- Start with [docs/START_HERE.md](docs/START_HERE.md) and the
  [learning path](docs/learning-path/README.md).
- For bugs, open an issue with a minimal repro and the affected package.
- For questions about the DI grammar, see
  [docs/DI_GRAMMAR.md](docs/DI_GRAMMAR.md) and
  [docs/EXTENSION_METHOD_INDEX.md](docs/EXTENSION_METHOD_INDEX.md).
