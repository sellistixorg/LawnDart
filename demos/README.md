# Demos

Hosts that exercise LawnDart. Academy is the guided tour. The other hosts show one setup each.

| Demo | What it shows | Store | Run |
|---|---|---|---|
| [Academy](LawnDart.Demo.Academy/README.md) | Hosted patterns, including aggregates, DCB, a live projection, and a task processor | InMemory. SQL Server by the `SqlServer` launch profile | `dotnet run --project demos/LawnDart.Demo.Academy` |
| [Academy WebApi](LawnDart.Demo.Academy.WebApi/README.md) | HTTP commands and projection queries | InMemory. SQL Server by the `SqlServer` launch profile | `dotnet run --project demos/LawnDart.Demo.Academy.WebApi` |
| [Multi-context InMemory](LawnDart.Demo.MultiContextInMemory/README.md) | Two bounded contexts with separate InMemory stores in one host | InMemory | `dotnet run --project demos/LawnDart.Demo.MultiContextInMemory` |
