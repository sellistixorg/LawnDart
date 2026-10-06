# Shop

A Blazor Server shop on one bounded context. It places orders on aggregates, reserves stock with a DCB entity, and pays then ships through reactors. Cookie users carry permissions and a tenant id. Stream ids are `{tenant}:{type}:{id}`.

## Run

InMemory is the default. No database.

```bash
dotnet run --project demos/LawnDart.Demo.Shop
```

Open http://localhost:5096 and choose a user.

Smoke the same host. It signs in, creates one unit of stock, races two buys, waits until the winner is shipped, and exits 0:

```bash
dotnet run --project demos/LawnDart.Demo.Shop --no-launch-profile -- --smoke
```

The smoke process listens on http://127.0.0.1:5096.

## Users

DEMO ONLY. These accounts have no passwords.

| User | Role | Tenant | What they can do |
|---|---|---|---|
| Alice | Customer | `buyers` | Place orders, view products |
| Maria | Customer | `buyers` | Place orders, view products |
| Bob | Seller | `bobs-store` | Create products, update price and stock, cancel orders |
| Larry | Seller | `larrys-emporium` | Same as Bob, in his own tenant |
| Carol | Warehouse | `system` | Ship orders, view inventory |

Alice and Maria share the `buyers` tenant. My Orders keeps each customer's rows by customer id. Bob and Larry write product streams under their own tenant prefix. A SKU is unique per seller tenant.

## What to click

| Page | What happens |
|---|---|
| Shop, Buy | `OrderAggregate` handles `PlaceOrderCommand` after `InventoryEntity` reserves stock |
| Shop, DCB Race | Two concurrent buys of the last unit. One append wins |
| My Orders | `OrderReactor` then `FulfillmentReactor`: place, pay, ship |
| Seller dashboard, Cancel | `CancelOrderCommand` requires `Order.Cancel` |
| Event Stream | Commands and the events they appended. Blue is a user command. Amber is a reactor or task |
| Warehouse | Carol sees every order |

`GET /api/views/fulfillment/{orderId}` is the multi-stream view (order events plus `StockReserved`). `GET /api/views/products` is the latest `AllProducts` projection. `GET /api/views/v1/products` and `GET /api/views/v2/products` are the versioned routes. Family metadata is `GET /api/views/metadata/products`.

Signed-in callers can list projection registrations at `GET /internal/projections/debug/projections`.

Command routes:

- `POST /api/product/create-product`
- `POST /api/product/update-price`
- `POST /api/product/update-stock`
- `POST /api/order/place-order`
- `POST /api/order/process-payment`
- `POST /api/order/ship-order`
- `POST /api/order/cancel-order`

Status codes: 202 accepted, 403 forbidden, 409 concurrency conflict, 422 domain rule.

## SQL Server

Create the database first. The host creates event, outbox, inbox, snapshot, and view tables at startup.

```bash
dotnet run --project demos/LawnDart.Demo.Shop --launch-profile SqlServer
```

Or pass `--sql`. The connection string is `ConnectionStrings:Shop`, then `LAWNDART_SQL_CONNECTION`.

The `SqlServer` launch profile uses this local Windows string:

`Server=localhost;Database=LawnDartShop;Trusted_Connection=True;TrustServerCertificate=True`

That profile turns on the transactional outbox and a SQL inbox. Reactors receive events from the outbox publisher. The InMemory profile publishes those events on the in-process transport directly.

Drop and recreate `LawnDartShop` to wipe SQL data. InMemory starts empty on each process.

## Snapshots

`InventoryEntity` and `SkuRegistryEntity` snapshot every 5 events. InMemory keeps those snapshots in process. SQL Server stores them in the context schema.
