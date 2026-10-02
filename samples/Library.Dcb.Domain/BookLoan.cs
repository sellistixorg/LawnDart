using LawnDart;
using LawnDart.Dcb;

namespace Library.Dcb.Domain;

public sealed class BookLoan : DcbEntity<BookLoanState>
{
    public const int MaxLoansPerMember = 3;

    public static string[] GetTags(Guid bookId, Guid memberId)
        => [$"book:{bookId}", $"member:{memberId}"];

    public void Handle(AddBookCommand cmd)
    {
        if (State.Exists)
            throw new DomainException("Book already exists.");

        Emit(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, cmd.BookId, cmd.Title, cmd.Isbn),
            $"book:{cmd.BookId}");
    }

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

    public void Handle(ReturnBookCommand cmd)
    {
        if (!State.Exists)
            throw new DomainException("Book does not exist.");
        if (!State.OnLoan)
            throw new DomainException("Book is not on loan.");

        Emit(new BookReturned(Guid.NewGuid(), DateTime.UtcNow, cmd.BookId, cmd.MemberId),
            $"book:{cmd.BookId}", $"member:{cmd.MemberId}");
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
                State.BorrowedByMemberId = null;
                break;
            case BookBorrowed e:
                State.MemberActiveLoans++;
                if (State.BookId == Guid.Empty || e.BookId == State.BookId)
                {
                    State.OnLoan = true;
                    State.BorrowedBy = e.MemberName;
                    State.BorrowedByMemberId = e.MemberId;
                }
                break;
            case BookReturned e:
                State.MemberActiveLoans = Math.Max(0, State.MemberActiveLoans - 1);
                if (e.BookId == State.BookId)
                {
                    State.OnLoan = false;
                    State.BorrowedBy = null;
                    State.BorrowedByMemberId = null;
                }
                break;
        }
    }
}
