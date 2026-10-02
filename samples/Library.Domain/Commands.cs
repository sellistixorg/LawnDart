using LawnDart;

namespace Library.Domain;

public sealed record AddBookCommand(Guid Id, Guid BookId, string Title, string Isbn) : ICommand;

public sealed record BorrowBookCommand(Guid Id, Guid BookId, string MemberName) : ICommand;

public sealed record ReturnBookCommand(Guid Id, Guid BookId) : ICommand;

public sealed record NotifyMemberCommand(Guid Id, Guid BookId, string MemberName) : ICommand;
