# LawnDart Academy

Zero-infrastructure showcase of the nine common command–event–state patterns.
Requires the .NET 10 SDK only. No Docker.

## Console demo

```bash
dotnet run --project demos/LawnDart.Demo.Academy
```

Non-interactive (CI / smoke test):

```bash
dotnet run --project demos/LawnDart.Demo.Academy -- --run-all
```

Optional SQL Server (requires a running instance and a connection string):

```bash
dotnet run --project demos/LawnDart.Demo.Academy --launch-profile SqlServer
```

## Web API

```bash
dotnet run --project demos/LawnDart.Demo.Academy.WebApi
```

Separate host (no shared project). Scalar at `/scalar/v1`. You mint the JWT
yourself — see [WebApi README](../LawnDart.Demo.Academy.WebApi/README.md).

## Showcases

| Key | Pattern |
|---|---|
| 1 | Traditional aggregate root + EDA choreography |
| 2 | DCB in-process (atomic append, no broker) |
| 3 | Live Lightweight projections |
| 4 | InMemory throughput |
| 5 | Task processor (state → command) |
| 6 | Temporal queries |
