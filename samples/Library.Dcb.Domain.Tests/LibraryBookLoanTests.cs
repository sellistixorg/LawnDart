using LawnDart;
using LawnDart.Dcb;
using LawnDart.Testing.Bdd;
using Library.Dcb.Domain;
using Library.Dcb.Host;
using Microsoft.Extensions.DependencyInjection;

namespace Library.Dcb.Domain.Tests;

public sealed class LibraryBookLoanTests
{
    [Fact]
    public async Task borrow_emits_event()
    {
        await using var ctx = BddTestContext.CreateInMemory(typeof(BookAdded), typeof(BookBorrowed), typeof(BookReturned));
        var bookId = Guid.NewGuid();
        var memberId = Guid.NewGuid();
        var tags = BookLoan.GetTags(bookId, memberId);

        await DcbSpec
            .For<BookLoan, BorrowBookCommand>(ctx, tags, new BorrowBookCommand(Guid.NewGuid(), bookId, memberId, "Jeremy"))
            .Given(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "A Tale of Two Cities", "ABCD"), tags)
            .ThenEmittedEvent<BookBorrowed>(e => e.BookId == bookId && e.MemberId == memberId && e.MemberName == "Jeremy")
            .AndAssert(result =>
            {
                Assert.True(result.Entity.State.OnLoan);
                Assert.Equal("Jeremy", result.Entity.State.BorrowedBy);
                Assert.Equal(1, result.Entity.State.MemberActiveLoans);
            })
            .RunAsync();
    }

    [Fact]
    public async Task return_clears_loan()
    {
        await using var ctx = BddTestContext.CreateInMemory(typeof(BookAdded), typeof(BookBorrowed), typeof(BookReturned));
        var bookId = Guid.NewGuid();
        var memberId = Guid.NewGuid();
        var tags = BookLoan.GetTags(bookId, memberId);

        await DcbSpec
            .For<BookLoan, ReturnBookCommand>(ctx, tags, new ReturnBookCommand(Guid.NewGuid(), bookId, memberId))
            .Given(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "A Tale of Two Cities", "ABCD"), tags)
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, bookId, memberId, "Jeremy"), tags)
            .ThenEmittedEvent<BookReturned>(e => e.BookId == bookId && e.MemberId == memberId)
            .AndAssert(result =>
            {
                Assert.False(result.Entity.State.OnLoan);
                Assert.Null(result.Entity.State.BorrowedBy);
                Assert.Equal(0, result.Entity.State.MemberActiveLoans);
            })
            .RunAsync();
    }

    [Fact]
    public async Task cannot_borrow_when_already_on_loan()
    {
        await using var ctx = BddTestContext.CreateInMemory(typeof(BookAdded), typeof(BookBorrowed), typeof(BookReturned));
        var bookId = Guid.NewGuid();
        var memberId = Guid.NewGuid();
        var tags = BookLoan.GetTags(bookId, memberId);

        await DcbSpec
            .For<BookLoan, BorrowBookCommand>(
                ctx,
                tags,
                new BorrowBookCommand(Guid.NewGuid(), bookId, memberId, "Someone Else"))
            .Given(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "Pragmatic Programmer", "978-0135957059"), tags)
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, bookId, Guid.NewGuid(), "Jane Doe"), tags)
            .ThenThrows<DomainException>()
            .AndAssert(result =>
                Assert.Equal("Book is already on loan.", result.Exception!.Message))
            .RunAsync();
    }

    [Fact]
    public async Task cannot_borrow_when_member_at_loan_limit()
    {
        await using var ctx = BddTestContext.CreateInMemory(typeof(BookAdded), typeof(BookBorrowed), typeof(BookReturned));
        var bookId = Guid.NewGuid();
        var memberId = Guid.NewGuid();
        var tags = BookLoan.GetTags(bookId, memberId);

        await DcbSpec
            .For<BookLoan, BorrowBookCommand>(
                ctx,
                tags,
                new BorrowBookCommand(Guid.NewGuid(), bookId, memberId, "Jeremy"))
            .Given(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "A Tale of Two Cities", "ABCD"), tags)
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, Guid.NewGuid(), memberId, "Jeremy"), tags)
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, Guid.NewGuid(), memberId, "Jeremy"), tags)
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, Guid.NewGuid(), memberId, "Jeremy"), tags)
            .ThenThrows<DomainException>()
            .AndAssert(result =>
                Assert.Equal("Member is at the loan limit.", result.Exception!.Message))
            .RunAsync();
    }

    [Fact]
    public void add_dcb_library_registers()
    {
        var services = new ServiceCollection();
        LibraryDcbHost.AddDcbLibrary(services);
        using var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetService<IDcbRepository>());
        Assert.NotNull(sp.GetService<ICommandHandler<BorrowBookCommand>>());
    }
}
