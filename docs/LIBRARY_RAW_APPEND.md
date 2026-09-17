# Library domain — raw append (comparison foil)

This page is **article material**, not the recommended path. The canonical
reference slice is [`samples/Library.Domain`](https://github.com/sellistixorg/LawnDart/tree/main/samples/Library.Domain) and
its given / when / then specs. Do not imitate this page when adding a slice.

It exists so an article can show both sides of the same book domain: raw
store internals (this page) versus commands, `Handle`, `BookState` ≠
`BookCatalogView`, and `AggregateSpec` + `AndView` (the slice).

Compared posts: [Chronicle](https://blog.cratis.io/event-sourcing-in-dotnet-with-chronicle/),
[Marten](https://jasperfx.net/news/chronicle-quickstart-on-marten).

Use the events and `LibraryProjector` already compiled in
`samples/Library.Domain`. Do not invent a second `BookAdded` or a second
projector. This foil’s stream id is `book:{guid:N}`; `AggregateSpec` writes
`Book:{guid}`. They are not interchangeable.

App code should live on `GetOrCreateAsync` / `HandleCommandAsync`
([QUICKSTART](QUICKSTART.md)), not here. `AppendAsync` is the correct store
API and the wrong first application lesson.

## Append and read

Chronicle: `EventLog.Append`. Marten: `StartStream` / `Append` /
`SaveChanges`. LawnDart store equivalent:

```csharp
using LawnDart;
using LawnDart.EventSourcing.EventStore;
using Library.Domain;
using Library.Projections;

var store = new InMemoryEventStore();

var bookId = Guid.NewGuid();
var streamId = $"book:{bookId:N}";

await store.AppendAsync(
    streamId,
    new IEvent[]
    {
        new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "A Tale of Two Cities", "ABCD"),
        new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, bookId, "Jeremy"),
    });

await store.AppendAsync(
    streamId,
    new IEvent[]
    {
        new BookReturned(Guid.NewGuid(), DateTime.UtcNow, bookId),
    },
    expectedVersion: 2);

var recorded = await store.ReadStreamAsync(streamId);
foreach (var envelope in recorded)
    Console.WriteLine($"{envelope.Version}: {envelope.Event.GetType().Name}");
```

Wrong `expectedVersion` throws `ConcurrencyException`. There is no command
and no `Handle` — the decide step is in the script.

## Fold by hand (Marten-shaped blur)

One collapsed type, not `BookState` / `BookCatalogView`. Useful only to say
you *can* do this; the slice does not.

```csharp
public sealed class BookSnapshot
{
    public Guid BookId { get; set; }
    public string Title { get; set; } = "";
    public string Isbn { get; set; } = "";
    public bool OnLoan { get; set; }
    public string? BorrowedBy { get; set; }
}

public static BookSnapshot FoldBook(IReadOnlyList<SequencedEvent> history)
{
    var book = new BookSnapshot();
    foreach (var envelope in history)
    {
        switch (envelope.Event)
        {
            case BookAdded e:
                book.BookId = e.BookId;
                book.Title = e.Title;
                book.Isbn = e.Isbn;
                book.OnLoan = false;
                book.BorrowedBy = null;
                break;
            case BookBorrowed e:
                book.OnLoan = true;
                book.BorrowedBy = e.MemberName;
                break;
            case BookReturned:
                book.OnLoan = false;
                book.BorrowedBy = null;
                break;
        }
    }
    return book;
}
```

## Project the same read model

Push the stream through the slice’s projector:

```csharp
var projector = new LibraryProjector();
foreach (var envelope in await store.ReadStreamAsync(streamId))
    projector.Apply(envelope.Event);

var view = projector.GetBook(bookId);
Console.WriteLine($"Catalog: {view?.Title} OnLoan={view?.OnLoan}");
Console.WriteLine($"Borrowed count: {projector.BorrowedBooks.Count}");
```
