# Step 2 — First aggregate

**Previous:** [Concepts](01-concepts.md) · **Next:** [Testing](03-testing-your-aggregate.md)

The shortest copy-paste quick start stays on the [README](https://github.com/sellistixorg/LawnDart/blob/main/README.md)
and [Quickstart](../QUICKSTART.md). This step is the library domain — a
`Book` with an invariant (`OnLoan`) whose decision state is not the catalog
view.

Excerpted from `samples/Library.Domain/Commands.cs`, `Events.cs`,
`BookState.cs`, and `Book.cs`.

## Types

```csharp
public sealed record AddBookCommand(Guid Id, Guid BookId, string Title, string Isbn) : ICommand;
public sealed record BorrowBookCommand(Guid Id, Guid BookId, string MemberName) : ICommand;
public sealed record ReturnBookCommand(Guid Id, Guid BookId) : ICommand;

[EventTypeName("book-added")]
public sealed record BookAdded(
    Guid Id,
    DateTime Timestamp,
    Guid BookId,
    string Title,
    string Isbn) : IEvent;

[EventTypeName("book-borrowed")]
public sealed record BookBorrowed(
    Guid Id,
    DateTime Timestamp,
    Guid BookId,
    string MemberName) : IEvent;

[EventTypeName("book-returned")]
public sealed record BookReturned(
    Guid Id,
    DateTime Timestamp,
    Guid BookId) : IEvent;

public sealed class BookState : IState
{
    public Guid BookId { get; set; }
    public bool Exists { get; set; }
    public bool OnLoan { get; set; }
    public string? BorrowedBy { get; set; }
}

public sealed class Book : AggregateRoot<BookState>
{
    public void Handle(AddBookCommand cmd)
    {
        if (State.Exists)
            throw new InvalidOperationException("Book already exists.");

        Apply(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, cmd.BookId, cmd.Title, cmd.Isbn));
    }

    public void Handle(BorrowBookCommand cmd)
    {
        if (!State.Exists)
            throw new InvalidOperationException("Book does not exist.");
        if (State.OnLoan)
            throw new InvalidOperationException("Book is already on loan.");

        Apply(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, cmd.BookId, cmd.MemberName));
    }

    public void Handle(ReturnBookCommand cmd)
    {
        if (!State.Exists)
            throw new InvalidOperationException("Book does not exist.");
        if (!State.OnLoan)
            throw new InvalidOperationException("Book is not on loan.");

        Apply(new BookReturned(Guid.NewGuid(), DateTime.UtcNow, cmd.BookId));
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
```

Keep `Id` and `Timestamp` first on events if you follow the Eventhesis
field-order convention. Every concrete `IEvent` needs `[EventTypeName]`.
`BookState` is decision state only — no title, no ISBN. Those live on
`BookCatalogView` ([step 4](04-reading-state.md)).

## Host and dispatch

Excerpted from `samples/Library.Host/LibraryHost.cs` and
`samples/Library.Domain/Handlers.cs`.

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddSingleton<ITenantContextProvider, AmbientTenantContextProvider>();
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory();
ctx.WithCommandHandlers<BorrowBookHandler>();
ctx.WithEventTypes<BookAdded>();
```

```csharp
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

`UseInMemory()` on the `"default"` context registers unkeyed aliases, so
`GetRequiredService<IAggregateRepository>()` resolves without a key. Multi-context
hosts still use `GetRequiredKeyedService<IAggregateRepository>("other")`.

Guid overloads build `{type}:{id}` (or `{tenant}:{type}:{id}`). If the stream
is a custom ID, use `GetOrCreateAsync<T>(streamId)` instead.

Do not load the store yourself. Use `GetAsync` / `GetOrCreateAsync` and
`HandleCommandAsync`. Do not call `SetStreamId`, `SetVersion`,
`SetCommittedVersion`, or `ReplayEvents` from application code.

Aggregates record events with `Apply`. DCB entities use `Emit` (tags).
`Handle(TCommand)` is authoring; `HandleCommandAsync` is the repository.
See [Intentional verb differences](../GLOSSARY.md#intentional-verb-differences).

Reference slice: `samples/Library.Domain`. Runnable host: Academy
(`demos/LawnDart.Demo.Academy`).
