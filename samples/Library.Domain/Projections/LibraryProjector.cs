using LawnDart;
using Library.Domain;

namespace Library.Projections;

public sealed class BookCatalogView
{
    public Guid BookId { get; set; }
    public string Title { get; set; } = "";
    public string Isbn { get; set; } = "";
    public bool OnLoan { get; set; }
    public string? BorrowedBy { get; set; }
}

/// <summary>Who has what out right now — instance removed on return.</summary>
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
