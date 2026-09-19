using LawnDart;
using LawnDart.Dcb;

namespace Library.Dcb.Domain;

public sealed class AddBookHandler : ICommandHandler<AddBookCommand>
{
    private readonly IDcbRepository _loans;

    public AddBookHandler(IDcbRepository loans) => _loans = loans;

    public async Task HandleAsync(AddBookCommand command, CancellationToken cancellationToken = default)
    {
        var loan = await _loans.GetOrCreateEntityAsync<BookLoan>(
            [$"book:{command.BookId}"], cancellationToken).ConfigureAwait(false);
        await _loans.HandleCommandAsync(loan, command, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}

public sealed class BorrowBookHandler : ICommandHandler<BorrowBookCommand>
{
    private readonly IDcbRepository _loans;

    public BorrowBookHandler(IDcbRepository loans) => _loans = loans;

    public async Task HandleAsync(BorrowBookCommand command, CancellationToken cancellationToken = default)
    {
        var loan = await _loans.GetOrCreateEntityAsync<BookLoan>(
            BookLoan.GetTags(command.BookId, command.MemberId), cancellationToken).ConfigureAwait(false);
        await _loans.HandleCommandAsync(loan, command, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}

public sealed class ReturnBookHandler : ICommandHandler<ReturnBookCommand>
{
    private readonly IDcbRepository _loans;

    public ReturnBookHandler(IDcbRepository loans) => _loans = loans;

    public async Task HandleAsync(ReturnBookCommand command, CancellationToken cancellationToken = default)
    {
        var loan = await _loans.GetOrCreateEntityAsync<BookLoan>(
            BookLoan.GetTags(command.BookId, command.MemberId), cancellationToken).ConfigureAwait(false);
        await _loans.HandleCommandAsync(loan, command, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
