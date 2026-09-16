using LawnDart;
using LawnDart.Aggregates;

namespace Library.Domain;

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
