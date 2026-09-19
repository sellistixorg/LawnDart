using LawnDart;

namespace Library.Dcb.Domain;

public sealed class BookLoanState : IState
{
    public Guid BookId { get; set; }
    public bool Exists { get; set; }
    public bool OnLoan { get; set; }
    public string? BorrowedBy { get; set; }
    public Guid? BorrowedByMemberId { get; set; }
    public int MemberActiveLoans { get; set; }
}
