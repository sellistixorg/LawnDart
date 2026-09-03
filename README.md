# LawnDart

In-process .NET library for event-sourced **commands**, **events**, and **state**.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Version](https://img.shields.io/badge/version-0.1.0--alpha-orange)](CHANGELOG.md)

LawnDart is a standalone command–event–state runtime: aggregates and Dynamic
Consistency Boundaries, an event store (InMemory or SQL Server), lightweight
projections, messaging, ASP.NET Core command mapping, and a given/when/then
test harness.

Packages are not published to nuget.org yet (`0.1.0-alpha`). Clone this
repository and use project references, or pack to a local feed.

## Requirements

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- Docker Desktop — only for SQL Server integration tests (Testcontainers)

## Features

- Command / event / state types and the nine CES patterns (aggregates, DCB,
  projections, reactions, processors)
- `AddBoundedContext` + `UseInMemory()` with no infrastructure
- Durable SQL Server store, outbox, and lightweight projections
- ASP.NET Core HTTP command mapping and optional claims-based authorization
- In-process messaging and `LawnDart.Testing` BDD harnesses

## Packages

| Package | Role |
|---|---|
| `LawnDart` | Core contracts (`ICommand`, `IEvent`, `IState`, aggregates, DCB) |
| `LawnDart.EventSourcing` | Runtime and InMemory event store |
| `LawnDart.EventSourcing.SqlServer` | Durable SQL Server store |
| `LawnDart.Projections.Lightweight` | Read-model host |
| `LawnDart.Messaging` / `LawnDart.Messaging.InMemory` | Reactors and in-process transport |
| `LawnDart.AspNetCore` | HTTP command mapping |
| `LawnDart.Authorization.AspNetCore` | HTTP claims → authorization context |
| `LawnDart.Testing` | Given / when / then harnesses |

## Install (this repository)

```bash
git clone https://github.com/sellistix/LawnDart.git
cd LawnDart
dotnet restore
dotnet build
```

Reference the projects you need, for example:

```xml
<ItemGroup>
  <ProjectReference Include="path/to/LawnDart/src/LawnDart/LawnDart.csproj" />
  <ProjectReference Include="path/to/LawnDart/src/LawnDart.EventSourcing/LawnDart.EventSourcing.csproj" />
</ItemGroup>
```

To consume packed nupkgs from a local feed:

```bash
dotnet pack LawnDart.sln -c Release -o ./nupkgs
dotnet nuget add source ./nupkgs --name lawndart-local
```

## Quick start

Register the host:

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default").UseInMemory();
```

Run Academy (zero infrastructure):

```bash
dotnet run --project demos/LawnDart.Demo.Academy
```

Optional SQL Server profile and WebApi are documented in [docs/QUICKSTART.md](docs/QUICKSTART.md).
Backend choice: [docs/BACKEND_SELECTION.md](docs/BACKEND_SELECTION.md).

## Documentation

- [Start here](docs/START_HERE.md)
- [Overview](docs/OVERVIEW.md)
- [Quickstart](docs/QUICKSTART.md)
- [Learning path](docs/learning-path/README.md)
- [Glossary](docs/GLOSSARY.md)

## Status

Pre-1.0. Public API may change. See [CHANGELOG.md](CHANGELOG.md).

## Modelling companion

[Eventhesis](https://eventhesis.com) is a separate event-modelling tool that can
target LawnDart. You do not need it to use this library.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Please follow the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Security

Report vulnerabilities to **security@sellistix.com**. Do not open a public
issue. Details: [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE) © 2026 Sellistix
