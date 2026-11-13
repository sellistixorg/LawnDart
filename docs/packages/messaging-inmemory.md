# LawnDart.Messaging.InMemory

In-process `IMessageTransport`. Academy Showcase A uses it for choreography
without a broker.

Take it for local work and tests. Use SQL Server or Postgres plus
`EnableOutbox` when the consumer is another process. Declare those events
with `PublishEvents`. `Translate` is the normal case. `Publish<T>()` sends an
event that already lives in a contracts project. InMemory publishes in-process.
`UseInMemory` can record outbox messages when `EnableOutbox` is true. See
[Outbox pattern](../OUTBOX_PATTERN.md).

## Registration

```csharp
services.AddInMemoryMessaging();
```

That registers the transport and calls `AddMessaging()`. Then add reactors
or processors from `LawnDart.Messaging`.

A handler that throws is called again until `MaxDeliveryAttempts` (default 5).
The last exception is then thrown to the caller. Set `DeadLetterFailedMessages`
to true to park the message instead. Set `SetupTopology` to true to record one
in-process queue per declared message type, named `{token}/{subscription}`.
Concurrency and prefetch are checked at startup. Delivery still runs on the
publish call.

`AddSqlInboxStore(connectionString)` replaces the inbox this method
registers. The transport stays in-process. See
[SQL Server inbox](messaging-sqlserver.md).

## Related

- [Package map](README.md)
- [Messaging](messaging.md)
- [EDA patterns](../EDA-Patterns.md)
- [Outbox](../OUTBOX_PATTERN.md)
