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

Every `PublishAsync` throws when that message type has no subscriber in this
process. The caller still has the message. When that subscriber stops, the
next publish has no listener and throws. A queue on a broker is a different
check: a stopped consumer whose queue still exists still receives.
Startup counts in-process subscribers for each published type. `Warn` logs
a missing subscriber and the host still starts. `Throw` stops host startup
when that count is zero. See [Outbox pattern](../OUTBOX_PATTERN.md).
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
