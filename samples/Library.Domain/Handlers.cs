using LawnDart;
using LawnDart.Aggregates;

namespace Library.Domain;

public sealed class AddBookHandler : ICommandHandler<AddBookCommand>
{
    private readonly IAggregateRepository _books;

    public AddBookHandler(IAggregateRepository books) => _books = books;

    public async Task HandleAsync(AddBookCommand command, CancellationToken cancellationToken = default)
    {
        var book = await _books.GetOrCreateAsync<Book>(command.BookId, cancellationToken).ConfigureAwait(false);
        await _books.HandleCommandAsync(book, command, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}

public sealed class BorrowBookHandler : ICommandHandler<BorrowBookCommand>
{
    private readonly IAggregateRepository _books;

    public BorrowBookHandler(IAggregateRepository books) => _books = books;

    public async Task HandleAsync(BorrowBookCommand command, CancellationToken cancellationToken = default)
    {
        var book = await _books.GetOrCreateAsync<Book>(command.BookId, cancellationToken).ConfigureAwait(false);
        await _books.HandleCommandAsync(book, command, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}

public sealed class NotifyMemberHandler : ICommandHandler<NotifyMemberCommand>
{
    public Task HandleAsync(NotifyMemberCommand command, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}

public sealed class ReturnBookHandler : ICommandHandler<ReturnBookCommand>
{
    private readonly IAggregateRepository _books;

    public ReturnBookHandler(IAggregateRepository books) => _books = books;

    public async Task HandleAsync(ReturnBookCommand command, CancellationToken cancellationToken = default)
    {
        var book = await _books.GetOrCreateAsync<Book>(command.BookId, cancellationToken).ConfigureAwait(false);
        await _books.HandleCommandAsync(book, command, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
