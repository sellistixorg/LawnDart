# LawnDart.Messaging.Postgres

Postgres inbox for `LawnDart.Messaging`. Reactor and event-processor
deduplication survives a restart and is shared by every instance on the
same table.

## What this project provides

- `PostgresInboxStore` (`IInboxStore`)
- `AddPostgresInboxStore` and `InitializePostgresInboxStoreAsync`

Pair it with `LawnDart.Messaging.InMemory`, `LawnDart.Messaging.RabbitMQ`,
`LawnDart.Messaging.AzureServiceBus`, or `LawnDart.Messaging.AwsSqs`. This package replaces the inbox.
The transport you registered stays.

The default schema is `lawndart`. The `public` schema is rejected.

Call `InitializePostgresInboxStoreAsync` from a deploy or admin step. See
[Schema initialization](https://github.com/sellistixorg/LawnDart/blob/main/docs/SCHEMA_INITIALIZATION.md).

See [the package page](https://github.com/sellistixorg/LawnDart/blob/main/docs/packages/messaging-postgres.md).
