# LawnDart.Messaging.SqlServer

SQL Server inbox for `LawnDart.Messaging`. Reactor and event-processor
deduplication survives a restart and is shared by every instance on the
same table.

## What this project provides

- `SqlServerInboxStore` (`IInboxStore`)
- `AddSqlInboxStore` and `InitializeSqlInboxStoreAsync`

Pair it with `LawnDart.Messaging.InMemory`, `LawnDart.Messaging.RabbitMQ`,
`LawnDart.Messaging.AzureServiceBus`, or `LawnDart.Messaging.AwsSqs`. This package replaces the inbox.
The transport you registered stays.
