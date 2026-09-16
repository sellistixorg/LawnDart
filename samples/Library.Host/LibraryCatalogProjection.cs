using Library.Domain;
using Library.Projections;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Sdk;

namespace Library.Host;

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
