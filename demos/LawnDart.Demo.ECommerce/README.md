# ECommerce

A cart, product, and order console. Academy is the guided tour. This host keeps the cart and order scenarios.

InMemory is the default. It needs no database.

## Run

```bash
dotnet run --project demos/LawnDart.Demo.ECommerce -- --run-all
```

One scenario:

```bash
dotnet run --project demos/LawnDart.Demo.ECommerce -- traditional
dotnet run --project demos/LawnDart.Demo.ECommerce -- dcb
dotnet run --project demos/LawnDart.Demo.ECommerce -- hybrid
dotnet run --project demos/LawnDart.Demo.ECommerce -- eda
dotnet run --project demos/LawnDart.Demo.ECommerce -- multicontext
```

`--run-all` runs those five and exits 0 when each one finishes its checks.

| Approach | What it does |
|---|---|
| traditional | `Cart`, `Order`, and `Product` aggregates. Hand-rolled projectors fold each stream. |
| dcb | The same lifecycle by tag. One append writes the fulfillment events together. A second append on the same order tag fails the condition. |
| hybrid | The product catalog is an aggregate. Warehouse stock and a transfer are tagged writes. The transfer is one append of two events. |
| eda | A reactor, an event processor, and a task processor on the in-process transport. Then a four-step choreography and one in-process pass. |
| multicontext | `ordering` and `catalog` each have their own store. Place and ship use `Order:{orderId}`. List and discontinue use `Product:{productId}`. |

Stream ids for aggregates are `{tenant}:Cart:{id}`, `{tenant}:Order:{id}`, and `{tenant}:Product:{id}`. The host tenant is `demo-tenant`.

This host sets `EnableAuthorization` to false, so the permission attributes on checkout and create-order are not checked. The Shop demo is the permissions host.

EDA dedup in this console uses the message id for the subscriber in that section. A hosted reactor keys the inbox by consumer type plus message id. `AddSqlInboxStore` is the SQL Server inbox. This walk keeps the in-memory inbox.

## SQL Server

Create the database first when you bring your own server. The host creates event and outbox tables at startup. These scenarios use hand-rolled projectors, so this host does not create projection view tables.

```bash
dotnet run --project demos/LawnDart.Demo.ECommerce -- --run-all --backend sqlserver
```

The connection string is `ConnectionStrings:ECommerce`, then `LAWNDART_SQL_CONNECTION`. When neither is set, `--backend sqlserver` starts a local container. That needs Docker.

The `SqlServer` launch profile uses this local Windows string:

`Server=localhost;Database=LawnDartECommerce;Trusted_Connection=True;TrustServerCertificate=True`

That profile turns on the transactional outbox. Reactors in the `eda` scenario still publish on the in-process transport. They do not read the outbox.

```bash
dotnet run --project demos/LawnDart.Demo.ECommerce -- outbox --backend sqlserver
```

`outbox` needs SQL Server. It creates an order, dead-letters the outbox row, resets it, and waits until the processor publishes that same row. InMemory does not commit an outbox row with the event.

Drop and recreate `LawnDartECommerce` to wipe SQL data. InMemory starts empty on each process.
