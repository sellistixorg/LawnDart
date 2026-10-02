---
name: lawndart-reactions
description: Author LawnDart reactions. IReactor plus AddInMemoryMessaging and AddReactor. Use when an event should emit follow-on commands.
---

# Reactions

`IReactor<TEvent>` turns an event into commands (broker transport).
`IDcbReactor` is the in-process tag/metadata twin. Register the broker
path with `AddInMemoryMessaging()` and `AddReactor<TReactor, TEvent>()`.

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
