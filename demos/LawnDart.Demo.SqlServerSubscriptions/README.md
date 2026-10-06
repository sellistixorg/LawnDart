# SQL Server subscriptions

Catch-up, then poll-backed live delivery on `IEventStoreSubscriptions.Subscribe`.

Requires the .NET 10 SDK and a SQL Server you start yourself. Create the database first. The host creates tables in schema `subdemo` at startup. Schema init does not create the database.

```sql
CREATE DATABASE LawnDartSubDemo;
```

## Docker

```bash
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Your_password123" \
  -p 1433:1433 -d mcr.microsoft.com/mssql/server:2022-latest
```

The password `Your_password123` is DEMO ONLY.

## Run

Set `LAWNDART_SQL_CONNECTION`, or pass the connection string as the first argument.

```bash
export LAWNDART_SQL_CONNECTION="Server=localhost,1433;Database=LawnDartSubDemo;User Id=sa;Password=Your_password123;TrustServerCertificate=True"
dotnet run --project demos/LawnDart.Demo.SqlServerSubscriptions
```

```bash
dotnet run --project demos/LawnDart.Demo.SqlServerSubscriptions -- "Server=localhost,1433;Database=LawnDartSubDemo;User Id=sa;Password=Your_password123;TrustServerCertificate=True"
```

A missing connection string prints help and exits 1.

## What to look for

Each run appends three events, then subscribes from the first of those three. One more append follows. Live delivery polls every 75 ms. The run prints three `catch-up` lines and one `live` line, then `Done (3 catch-up + 1 live via poll).` Exit code is 0.

A second run appends three new events. Sequences continue from the stored head. The same three catch-up lines and one live line print again.

The method table for `IEventStoreSubscriptions` is in [Backend selection](../../docs/BACKEND_SELECTION.md#portable-store-contract).
