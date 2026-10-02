---
name: lawndart-domain-model
description: Author LawnDart commands, events, aggregates, and DCB entities. Use when adding or changing domain types, stream IDs, or tags.
---

# Domain model

App-facing dispatch is `ICommandHandler<T>`. Aggregates and DCB entities
declare closed `Handle(TCommand)`. Persist with `HandleCommandAsync`.

**Load:** aggregate `GetOrCreateAsync<T>(id)` when the stream is
`{type}:{guid}` (or `{tenant}:{type}:{guid}`); `GetOrCreateAsync<T>(streamId)`
otherwise. DCB `GetOrCreateEntityAsync<T>(tags)` then `Emit(event, tags)`.
Do not implement `IProjector<TState>`.

Stream IDs: `{tenant}:{type}:{id}`. Tags for DCB: `type:{id}` (Guids, not
names). See `docs/STREAM_IDS.md` and `docs/TAGGING.md`.

Every written `IEvent` needs `[EventTypeName]`. `Id` and `Timestamp` are
required properties. Declaration order is an Eventhesis convention, not a
runtime rule. See `docs/EVENTHESIS.md` and `docs/EVENT_SCHEMA_VERSIONING.md`.

## Aggregate

Excerpt from `samples/Library.Domain/Commands.cs`, `Events.cs`, `BookState.cs`,
`Book.cs`, `Handlers.cs`:

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
            throw new DomainException("Book does not exist.");
        if (State.OnLoan)
            throw new DomainException("Book is already on loan.");

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

## DCB

DCB when a rule spans identities. `DcbEntity` + `IDcbRepository`, not two
aggregates plus a distributed transaction. Aggregates `Apply` events; DCB
entities `Emit` with tags. Input: `build-kit/library-dcb-slice.json`.

Excerpt from `samples/Library.Dcb.Domain/BookLoan.cs`, `Handlers.cs`:

```csharp
public sealed class BookLoan : DcbEntity<BookLoanState>
{
    public const int MaxLoansPerMember = 3;

    public static string[] GetTags(Guid bookId, Guid memberId)
        => [$"book:{bookId}", $"member:{memberId}"];

    public void Handle(BorrowBookCommand cmd)
    {
        if (!State.Exists)
            throw new DomainException("Book does not exist.");
        if (State.OnLoan)
            throw new DomainException("Book is already on loan.");
        if (State.MemberActiveLoans >= MaxLoansPerMember)
            throw new DomainException("Member is at the loan limit.");

        Emit(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, cmd.BookId, cmd.MemberId, cmd.MemberName),
            $"book:{cmd.BookId}", $"member:{cmd.MemberId}");
    }
}

public sealed class BorrowBookHandler : ICommandHandler<BorrowBookCommand>
{
    private readonly IDcbRepository _loans;

    public BorrowBookHandler(IDcbRepository loans) => _loans = loans;

    public async Task HandleAsync(BorrowBookCommand command, CancellationToken cancellationToken = default)
    {
        var loan = await _loans.GetOrCreateEntityAsync<BookLoan>(
            BookLoan.GetTags(command.BookId, command.MemberId), cancellationToken).ConfigureAwait(false);
        await _loans.HandleCommandAsync(loan, command, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
```

Broker reactions are `IReactor`; in-process DCB reactions are `IDcbReactor`.
See `lawndart-reactions` and `docs/DCB_PATTERNS.md`.

Read models are not part of the aggregate. Use Lightweight
`ProjectionBase<TView>` (see `lawndart-projection-authoring`) or your own
projector (`LibraryProjector` in `samples/Library.Domain/Projections`).
