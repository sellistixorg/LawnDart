# EDA patterns

Event-driven choreography uses `LawnDart.Messaging` plus a transport.
`LawnDart.Messaging.InMemory` delivers in-process.
`LawnDart.Messaging.RabbitMQ` delivers through RabbitMQ.
`LawnDart.Messaging.AzureServiceBus` delivers through Azure Service Bus.
`LawnDart.Messaging.AwsSqs` delivers through Amazon SQS. SNS fans each topic out to one queue per subscription.
See [Message transports](MESSAGE_TRANSPORTS.md).

## Reaction (Event → Command)

Academy `PaymentReactor`:

```csharp
public sealed class PaymentReactor : IReactor<SeatReservationConfirmed>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        SeatReservationConfirmed @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
    {
        IEnumerable<ICommand> commands =
        [
            new ProcessPaymentCommand(Guid.NewGuid(), @event.StudentId, @event.CourseId, @event.Amount)
        ];
        return Task.FromResult(commands);
    }
}

services.AddReactor<PaymentReactor, SeatReservationConfirmed>();
```

`IReactor<TEvent>` is the broker path (`MessageContext`). `IDcbReactor`
is the in-process DCB path (`EventMetadata`, `GetTagsForReaction()`).
`GetTagsForReaction()` returns the same strings the context
`ITagFormat` builds. See [Tagging](TAGGING.md) and
[Intentional verb differences](GLOSSARY.md#intentional-verb-differences).

Academy Showcase A wires `InMemoryMessageTransport` so the reservation
event becomes a payment command, then a confirmation command.

## Event processing (Event → Event)

`IEventProcessor<TEvent>` consumes an event and may emit further events
or side work without going through a command.

## Task processing (State → Command)

`ITaskProcessor` polls a read model and emits commands when a condition
is true (overdue registration, SLA timeout). Academy menu item 5
demonstrates this without a broker.

## Outbox

Use SQL Server plus `EnableOutbox` when the consumer is another process.
`AddMessageTransportOutboxPublisher` hands each row to the registered
transport: in-process, RabbitMQ, Azure Service Bus, or Amazon SQS.
See [OUTBOX_PATTERN.md](OUTBOX_PATTERN.md).

## Inbox

Hosted reactors and processors drop a second delivery of the same message.
The key is the consumer type name plus `MessageContext.MessageId`, so two
consumers of one message both run.

`AddInMemoryMessaging` keeps that record in the process.
`AddSqlInboxStore` stores it in SQL Server. Call
`InitializeSqlInboxStoreAsync` at startup. See
[SQL Server inbox](packages/messaging-sqlserver.md).
