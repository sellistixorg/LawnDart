using LawnDart;
using LawnDart.Messaging;
using LawnDart.Patterns.Reaction;

namespace Library.Domain;

public sealed class LoanNoticeReactor : IReactor<BookBorrowed>
{
    public Task<IEnumerable<ICommand>> ReactAsync(
        BookBorrowed @event,
        MessageContext context,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IEnumerable<ICommand>>(
            [new NotifyMemberCommand(Guid.NewGuid(), @event.BookId, @event.MemberName)]);
}
