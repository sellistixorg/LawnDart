using LawnDart;

namespace Library.Dcb.Domain;

public sealed record AddBookCommand(Guid Id, Guid BookId, string Title, string Isbn) : ICommand;

public sealed record BorrowBookCommand(Guid Id, Guid BookId, Guid MemberId, string MemberName) : ICommand;

public sealed record ReturnBookCommand(Guid Id, Guid BookId, Guid MemberId) : ICommand;
