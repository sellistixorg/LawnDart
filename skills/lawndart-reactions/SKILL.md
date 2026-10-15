---
name: lawndart-reactions
description: Author LawnDart reactions. IReactor plus AddReactor on the in-process, RabbitMQ, Azure Service Bus, or Amazon SQS transport. Use when an event should emit follow-on commands.
---

# Reactions

`IReactor<TEvent>` turns an event into commands (broker transport).
`IDcbReactor` is the in-process tag/metadata twin. Register the broker
path with `AddReactor<TReactor, TEvent>()` and a transport:
`AddInMemoryMessaging()`, `AddRabbitMqTransport`,
`AddAzureServiceBusTransport`, or `AddAwsSqsTransport`.
`AddSqlInboxStore(connectionString)` replaces the in-memory inbox when
dedup must survive a restart. Call `InitializeSqlInboxStoreAsync` at startup.
The excerpt below uses the in-process transport. A RabbitMQ host calls
`AddRabbitMqTransport` from `LawnDart.Messaging.RabbitMQ`. An Azure Service Bus
host calls `AddAzureServiceBusTransport` from `LawnDart.Messaging.AzureServiceBus`.
An Amazon SQS host calls `AddAwsSqsTransport` from `LawnDart.Messaging.AwsSqs`.
The event passed to `AddReactor` must declare `[EventTypeName]`.
The host fails at startup when the attribute is missing.

`IEventProcessor` (event to event) and `ITaskProcessor` (state to command)
are also hosted. See `docs/learning-path/06-reactions.md`.

Excerpt from `samples/Library.Domain/LoanNoticeReactor.cs`:

```csharp
public sealed class LoanNoticeReactor : IReactor<BookBorrowed>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        BookBorrowed @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IEnumerable<ICommand>>(
            [new NotifyMemberCommand(Guid.NewGuid(), @event.BookId, @event.MemberName)]);
}
```

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddInMemoryLibrary`):

```csharp
services.AddInMemoryMessaging();
services.AddReactor<LoanNoticeReactor, BookBorrowed>();
```
