# LawnDart.Messaging.SqlServer

SQL Server inbox for `LawnDart.Messaging`. Reactor and event-processor
deduplication survives a restart and is shared by every instance on the
same table.

## What this project provides

- `SqlServerInboxStore` (`IInboxStore`)
- `AddSqlInboxStore` and `InitializeSqlInboxStoreAsync`

The transport stays `LawnDart.Messaging.InMemory` unless you register your own
`IMessageTransport`.
