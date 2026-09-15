using LawnDart;

namespace Library.Domain;

public sealed class BookState : IState
{
    public Guid BookId { get; set; }
    public bool Exists { get; set; }
    public bool OnLoan { get; set; }
    public string? BorrowedBy { get; set; }
}
