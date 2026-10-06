# Demos

Hosts that exercise LawnDart. Academy is the guided tour. The other hosts show one setup each.

| Demo | What it shows | Store | Run |
|---|---|---|---|
| [Academy](LawnDart.Demo.Academy/README.md) | Hosted patterns, including aggregates, DCB, a live projection, and a task processor | InMemory. SQL Server by the `SqlServer` launch profile | `dotnet run --project demos/LawnDart.Demo.Academy` |
| [Academy WebApi](LawnDart.Demo.Academy.WebApi/README.md) | HTTP commands and projection queries | InMemory. SQL Server by the `SqlServer` launch profile | `dotnet run --project demos/LawnDart.Demo.Academy.WebApi` |
| [Multi-context InMemory](LawnDart.Demo.MultiContextInMemory/README.md) | Two bounded contexts with separate InMemory stores in one host | InMemory | `dotnet run --project demos/LawnDart.Demo.MultiContextInMemory` |
| [InMemory subscriptions](LawnDart.Demo.InMemorySubscriptions/README.md) | Catch-up, then live delivery, with a client checkpoint | InMemory | `dotnet run --project demos/LawnDart.Demo.InMemorySubscriptions` |
| [SQL Server subscriptions](LawnDart.Demo.SqlServerSubscriptions/README.md) | Catch-up, then poll-backed live delivery | SQL Server. Connection string from argv or `LAWNDART_SQL_CONNECTION` | `dotnet run --project demos/LawnDart.Demo.SqlServerSubscriptions` |
