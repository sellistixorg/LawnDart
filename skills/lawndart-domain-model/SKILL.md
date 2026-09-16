---
name: lawndart-domain-model
description: Author LawnDart commands, events, aggregates, DCB entities, tags, and stream IDs. Keep IEvent Id and Timestamp first.
---

# Domain model

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs).
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization. Do not write a per-command async Handle switch on the entity (obsolete).
3. **Load** with `GetOrCreateAsync<T>(id)` when the stream is `{type}:{guid}`; use `GetOrCreateAsync<T>(streamId)` otherwise.
4. **Projections:** `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
5. **Stores:** `UseInMemory` / `UseSqlServer` on `AddBoundedContext(name)`.

Excerpt from `samples/Library.Domain/Commands.cs`, `Events.cs`, `BookState.cs`, `Book.cs`, `Handlers.cs`:

```csharp
public sealed record BorrowBookCommand(Guid Id, Guid BookId, string MemberName) : ICommand;

[EventTypeName("book-borrowed")]
public sealed record BookBorrowed(
    Guid Id,
    DateTime Timestamp,
    Guid BookId,
    string MemberName) : IEvent;

public sealed class BookState : IState
{
    public Guid BookId { get; set; }
    public bool Exists { get; set; }
    public bool OnLoan { get; set; }
    public string? BorrowedBy { get; set; }
}

public sealed class Book : AggregateRoot<BookState>
{
    public void Handle(BorrowBookCommand cmd)
    {
        if (!State.Exists)
            throw new InvalidOperationException("Book does not exist.");
        if (State.OnLoan)
            throw new InvalidOperationException("Book is already on loan.");

        Apply(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, cmd.BookId, cmd.MemberName));
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        switch (@event)
        {
            case BookAdded e:
                State.BookId = e.BookId;
                State.Exists = true;
                State.OnLoan = false;
                State.BorrowedBy = null;
                break;
            case BookBorrowed e:
                State.OnLoan = true;
                State.BorrowedBy = e.MemberName;
                break;
            case BookReturned:
                State.OnLoan = false;
                State.BorrowedBy = null;
                break;
        }
    }
}

public sealed class BorrowBookHandler : ICommandHandler<BorrowBookCommand>
{
    private readonly IAggregateRepository _books;

    public BorrowBookHandler(IAggregateRepository books) => _books = books;

    public async Task HandleAsync(BorrowBookCommand command, CancellationToken cancellationToken = default)
    {
        var book = await _books.GetOrCreateAsync<Book>(command.BookId, cancellationToken).ConfigureAwait(false);
        await _books.HandleCommandAsync(book, command, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
```

`BookState` is decision state only. Catalog fields live on `BookCatalogView`.

Stream IDs: `{tenant}:{type}:{id}`. Tags for DCB: `type:{id}`.
Load with `GetOrCreateAsync<T>(id)` when that shape holds; use
`GetOrCreateAsync<T>(streamId)` for custom IDs. Do not call `SetStreamId`
or `ReplayEvents` in app code.

DCB when a rule spans identities — `DcbEntity` + `IDcbRepository`, not two
aggregates plus a distributed transaction. Aggregates `Apply` events; DCB
entities `Emit` with tags. Author `Handle(TCommand)`; persist with
`HandleCommandAsync`. Broker reactions are `IReactor`; in-process DCB
reactions are `IDcbReactor`. These names stay. See
`docs/GLOSSARY.md` (Intentional verb differences).

Read models are not part of the aggregate. Use Lightweight
`ProjectionBase<TView>` (see `lawndart-projection-authoring`) or your own
projector (`LibraryProjector` in `samples/Library.Domain/Projections`).
Do not implement `IProjector` unless you are writing your own
fold (`IProjector` is experimental). Multi-stream Lightweight views
implement `IMultiStreamEntityResolver`.

See `docs/STREAM_IDS.md`, `docs/TAGGING.md`, `docs/DCB_PATTERNS.md`.
