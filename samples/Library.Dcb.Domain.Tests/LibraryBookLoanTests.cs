using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventStore;
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

        await DcbSpec
            .For<BookLoan, BorrowBookCommand>(ctx, BookLoan.ForLoan(bookId, memberId), new BorrowBookCommand(Guid.NewGuid(), bookId, memberId, "Jeremy"))
            .Given(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "A Tale of Two Cities", "ABCD"), $"book:{bookId}")
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

        await DcbSpec
            .For<BookLoan, ReturnBookCommand>(ctx, BookLoan.ForLoan(bookId, memberId), new ReturnBookCommand(Guid.NewGuid(), bookId, memberId))
            .Given(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "A Tale of Two Cities", "ABCD"), $"book:{bookId}")
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, bookId, memberId, "Jeremy"), $"book:{bookId}", $"member:{memberId}")
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
        var borrowerId = Guid.NewGuid();

        await DcbSpec
            .For<BookLoan, BorrowBookCommand>(
                ctx,
                BookLoan.ForLoan(bookId, memberId),
                new BorrowBookCommand(Guid.NewGuid(), bookId, memberId, "Someone Else"))
            .Given(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "Pragmatic Programmer", "978-0135957059"), $"book:{bookId}")
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, bookId, borrowerId, "Jane Doe"), $"book:{bookId}", $"member:{borrowerId}")
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
        var otherA = Guid.NewGuid();
        var otherB = Guid.NewGuid();
        var otherC = Guid.NewGuid();

        await DcbSpec
            .For<BookLoan, BorrowBookCommand>(
                ctx,
                BookLoan.ForLoan(bookId, memberId),
                new BorrowBookCommand(Guid.NewGuid(), bookId, memberId, "Jeremy"))
            .Given(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "A Tale of Two Cities", "ABCD"), $"book:{bookId}")
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, otherA, memberId, "Jeremy"), $"book:{otherA}", $"member:{memberId}")
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, otherB, memberId, "Jeremy"), $"book:{otherB}", $"member:{memberId}")
            .Given(new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, otherC, memberId, "Jeremy"), $"book:{otherC}", $"member:{memberId}")
            .ThenThrows<DomainException>()
            .AndAssert(result =>
                Assert.Equal("Member is at the loan limit.", result.Exception!.Message))
            .RunAsync();
    }

    [Fact]
    public void for_loan_names_the_stored_loan_events()
    {
        var bookId = Guid.NewGuid();
        var memberId = Guid.NewGuid();
        var query = BookLoan.ForLoan(bookId, memberId);

        Assert.Equal(2, query.Items.Count);
        Assert.Null(query.Items[0].Types);
        Assert.Equal(new[] { $"book:{bookId}" }, query.Items[0].Tags);
        Assert.Equal(
            new[]
            {
                EventTypeCatalog.TryGetDeclaredName(typeof(BookBorrowed))!,
                EventTypeCatalog.TryGetDeclaredName(typeof(BookReturned))!,
            },
            query.Items[1].Types);
        Assert.Equal(new[] { $"member:{memberId}" }, query.Items[1].Tags);

        var book = BookLoan.ForBook(bookId);
        Assert.Single(book.Items);
        Assert.Null(book.Items[0].Types);
        Assert.Equal(new[] { $"book:{bookId}" }, book.Items[0].Tags);
    }

    [Fact]
    public async Task add_then_borrow_through_handlers()
    {
        await using var sp = OpenHost();
        var bookId = Guid.NewGuid();
        var memberId = Guid.NewGuid();

        await sp.GetRequiredService<ICommandHandler<AddBookCommand>>()
            .HandleAsync(new AddBookCommand(Guid.NewGuid(), bookId, "A Tale of Two Cities", "ABCD"));
        await sp.GetRequiredService<ICommandHandler<BorrowBookCommand>>()
            .HandleAsync(new BorrowBookCommand(Guid.NewGuid(), bookId, memberId, "Jeremy"));

        var loan = await sp.GetRequiredService<IDcbRepository>()
            .GetOrCreateEntityAsync<BookLoan>(BookLoan.ForLoan(bookId, memberId));
        Assert.True(loan.State.Exists);
        Assert.True(loan.State.OnLoan);
        Assert.Equal("Jeremy", loan.State.BorrowedBy);
        Assert.Equal(1, loan.State.MemberActiveLoans);

        var events = (await sp.GetRequiredService<IEventStore>()
            .ReadByQueryAsync(BookLoan.ForLoan(bookId, memberId))).Events;
        Assert.Equal(2, events.Count);
        Assert.IsType<BookAdded>(events[0].Event);
        Assert.Equal(new[] { $"book:{bookId}" }, events[0].Tags);
        var borrowed = Assert.IsType<BookBorrowed>(events[1].Event);
        Assert.Equal(bookId, borrowed.BookId);
        Assert.Equal(memberId, borrowed.MemberId);
        Assert.Equal("Jeremy", borrowed.MemberName);
        Assert.Equal(new[] { $"book:{bookId}", $"member:{memberId}" }, events[1].Tags);
    }

    [Fact]
    public async Task borrow_hits_loan_limit_across_books()
    {
        await using var sp = OpenHost();
        var memberId = Guid.NewGuid();
        var bookIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var add = sp.GetRequiredService<ICommandHandler<AddBookCommand>>();
        var borrow = sp.GetRequiredService<ICommandHandler<BorrowBookCommand>>();

        foreach (var bookId in bookIds)
            await add.HandleAsync(new AddBookCommand(Guid.NewGuid(), bookId, "Title", "ISBN"));

        for (var i = 0; i < 3; i++)
            await borrow.HandleAsync(new BorrowBookCommand(Guid.NewGuid(), bookIds[i], memberId, "Jeremy"));

        var ex = await Assert.ThrowsAsync<DomainException>(() =>
            borrow.HandleAsync(new BorrowBookCommand(Guid.NewGuid(), bookIds[3], memberId, "Jeremy")));
        Assert.Equal("Member is at the loan limit.", ex.Message);

        var blocked = await sp.GetRequiredService<IDcbRepository>()
            .GetOrCreateEntityAsync<BookLoan>(BookLoan.ForLoan(bookIds[3], memberId));
        Assert.True(blocked.State.Exists);
        Assert.False(blocked.State.OnLoan);
        Assert.Equal(3, blocked.State.MemberActiveLoans);

        var stored = (await sp.GetRequiredService<IEventStore>().ReadByQueryAsync(Query.All())).Events;
        Assert.Equal(7, stored.Count);
        for (var i = 0; i < 4; i++)
        {
            Assert.IsType<BookAdded>(stored[i].Event);
            Assert.Equal(new[] { $"book:{bookIds[i]}" }, stored[i].Tags);
        }

        for (var i = 0; i < 3; i++)
        {
            var borrowed = Assert.IsType<BookBorrowed>(stored[i + 4].Event);
            Assert.Equal(bookIds[i], borrowed.BookId);
            Assert.Equal(new[] { $"book:{bookIds[i]}", $"member:{memberId}" }, stored[i + 4].Tags);
        }
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

    private static ServiceProvider OpenHost()
    {
        var services = new ServiceCollection();
        LibraryDcbHost.AddDcbLibrary(services);
        return services.BuildServiceProvider();
    }
}
