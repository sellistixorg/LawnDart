# Step 4. Reading state

**Previous:** [Testing](03-testing-your-aggregate.md) · **Next:** [DCB](05-dcb-patterns.md)

Projections turn the event log into views. Decision state (`BookState`) is
not the catalog (`BookCatalogView`). `BookState` has `Exists` / `OnLoan` /
`BorrowedBy`. The view also has `Title` and `Isbn`, which the aggregate
never needed to decide a borrow.

You may hand-roll a projector or use the shipped host. LawnDart ships
`LawnDart.Projections.Lightweight` only. Author `ProjectionBase<TView>` plus
scope attributes. A view that spans stream types implements
`IMultiStreamEntityResolver` on the handler.

Hand-rolled fold, excerpted from
`samples/Library.Domain/Projections/LibraryProjector.cs`
(`BorrowedBookView` XML comment omitted):

```csharp
public sealed class BookCatalogView
{
    public Guid BookId { get; set; }
    public string Title { get; set; } = "";
    public string Isbn { get; set; } = "";
    public bool OnLoan { get; set; }
    public string? BorrowedBy { get; set; }
}

public sealed class BorrowedBookView
{
    public Guid BookId { get; set; }
    public string MemberName { get; set; } = "";
}

public sealed class LibraryProjector
{
    private readonly Dictionary<Guid, BookCatalogView> _catalog = new();
    private readonly Dictionary<Guid, BorrowedBookView> _borrowed = new();

    public void Apply(IEvent @event)
    {
        switch (@event)
        {
            case BookAdded e:
                _catalog[e.BookId] = new BookCatalogView
                {
                    BookId = e.BookId,
                    Title = e.Title,
                    Isbn = e.Isbn,
                    OnLoan = false
                };
                break;
            case BookBorrowed e:
                if (_catalog.TryGetValue(e.BookId, out var book))
                {
                    book.OnLoan = true;
                    book.BorrowedBy = e.MemberName;
                }
                _borrowed[e.BookId] = new BorrowedBookView
                {
                    BookId = e.BookId,
                    MemberName = e.MemberName
                };
                break;
            case BookReturned e:
                if (_catalog.TryGetValue(e.BookId, out var returned))
                {
                    returned.OnLoan = false;
                    returned.BorrowedBy = null;
                }
                _borrowed.Remove(e.BookId);
                break;
        }
    }

    public BookCatalogView? GetBook(Guid bookId) =>
        _catalog.TryGetValue(bookId, out var v) ? v : null;

    public IReadOnlyCollection<BorrowedBookView> BorrowedBooks => _borrowed.Values;
}
```

`AndView` registers `projector.Apply` so the same fold is asserted in GWT.
Excerpted from `samples/Library.Domain.Tests/LibraryBookTests.cs`:

```csharp
.AndView(r => r.Register(projector.Apply))
.AndAssert(result =>
{
    Assert.True(result.Aggregate.State.OnLoan);
    Assert.Equal("Jeremy", result.Aggregate.State.BorrowedBy);

    var view = projector.GetBook(bookId);
    Assert.NotNull(view);
    Assert.Equal("A Tale of Two Cities", view.Title);
    Assert.True(view.OnLoan);
    Assert.Single(projector.BorrowedBooks);
})
```

The Lightweight host, excerpted from
`samples/Library.Host/LibraryCatalogProjection.cs` and `LibraryHost.cs`:

```csharp
[SingleStreamProjection("BookCatalog", streamType: "Book")]
public sealed class LibraryCatalogProjection : ProjectionBase<BookCatalogView>
{
    public void Handle(BookAdded e)
    {
        State.BookId = e.BookId;
        State.Title = e.Title;
        State.Isbn = e.Isbn;
        State.OnLoan = false;
        State.BorrowedBy = null;
    }

    public void Handle(BookBorrowed e)
    {
        State.OnLoan = true;
        State.BorrowedBy = e.MemberName;
    }

    public void Handle(BookReturned e)
    {
        State.OnLoan = false;
        State.BorrowedBy = null;
    }
}
```

```csharp
services.AddInMemoryProjectionStores("default");
ctx.WithProjections(
    [typeof(LibraryCatalogProjection).Assembly],
    opts =>
    {
        opts.PollInterval = TimeSpan.FromMilliseconds(200);
        opts.CheckpointInterval = 100;
    });
app.MapProjectionQueries("default");
```

Details: [LIGHTWEIGHT_PROJECTIONS.md](../LIGHTWEIGHT_PROJECTIONS.md).
