# Library host

`LibraryHost` registers the Library slice.

In-memory: `AddInMemoryLibrary`.

SQL Server: `AddSqlLibrary`. Create the tables with `InitializeSqlLibraryAsync`
from a deploy or admin step.

Postgres: `AddPostgresLibrary`. Create the tables with
`InitializePostgresLibraryAsync`. That call is its own command. The app does
not create tables when it starts.

## Postgres on this machine

Start Postgres:

```bash
docker compose -f samples/Library.Host/docker-compose.yml up -d --wait
```

Create the tables. That command also records the catalog event names
(`book-added`, `book-borrowed`, `book-returned`) so the projection can run
before every one of those events has been stored.

```bash
dotnet run --project samples/Library.Host -- setup
```

Start the app. It listens on `http://127.0.0.1:5088`.

```bash
dotnet run --project samples/Library.Host
```

Add a book:

```bash
curl -X POST http://127.0.0.1:5088/api/domain/add-book \
  -H "Content-Type: application/json" \
  -d '{"id":"00000000-0000-0000-0000-000000000001","bookId":"00000000-0000-0000-0000-000000000002","title":"A Tale of Two Cities","isbn":"ABCD"}'
```

Borrow it:

```bash
curl -X POST http://127.0.0.1:5088/api/domain/borrow-book \
  -H "Content-Type: application/json" \
  -d '{"id":"00000000-0000-0000-0000-000000000003","bookId":"00000000-0000-0000-0000-000000000002","memberName":"Jeremy"}'
```

A handler that succeeds returns 202.

The connection string defaults to
`Host=localhost;Port=5432;Database=library;Username=library;Password=library`.
The tables live in schema `lawndart`. Set `ConnectionStrings__Library` or
`LAWNDART_LIBRARY_POSTGRES` to use another connection string.

Schema setup is a deploy or admin step. See
[Schema initialization](../../docs/SCHEMA_INITIALIZATION.md) and
[Postgres hosting](../../docs/POSTGRES_HOSTING.md).
