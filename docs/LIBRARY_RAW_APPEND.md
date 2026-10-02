# Library domain: raw append (comparison foil)

This page is article material. The recommended path is
[`samples/Library.Domain`](https://github.com/sellistixorg/LawnDart/tree/main/samples/Library.Domain)
and its given / when / then specs.

An article can show both sides of the same book domain: raw store internals
(this page) versus commands, `Handle`, `BookState` distinct from
`BookCatalogView`, and `AggregateSpec` plus `AndView` (the slice).

Compared posts: [Chronicle](https://blog.cratis.io/event-sourcing-in-dotnet-with-chronicle/),
[Marten](https://jasperfx.net/news/chronicle-quickstart-on-marten).

The events and `LibraryProjector` are already compiled in
`samples/Library.Domain`. This foil's stream id is `book:{guid:N}`.
`AggregateSpec` writes `Book:{guid}`. They are not interchangeable.

Application code should use `GetOrCreateAsync` / `HandleCommandAsync`
([QUICKSTART](QUICKSTART.md)). `AppendAsync` is the store API. It is the
wrong first application lesson.

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

Wrong `expectedVersion` throws `ConcurrencyException`. The script calls
`AppendAsync`. The decide step is in the script, not in `Handle`.

## Fold by hand (Marten-shaped blur)

One collapsed type, not `BookState` / `BookCatalogView`. Useful only to show
you can fold this way. The slice does not.

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

Push the stream through the slice's projector:

```csharp
var projector = new LibraryProjector();
foreach (var envelope in await store.ReadStreamAsync(streamId))
    projector.Apply(envelope.Event);

var view = projector.GetBook(bookId);
Console.WriteLine($"Catalog: {view?.Title} OnLoan={view?.OnLoan}");
Console.WriteLine($"Borrowed count: {projector.BorrowedBooks.Count}");
```
