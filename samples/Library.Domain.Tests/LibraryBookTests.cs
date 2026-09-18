using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Testing.Bdd;
using Library.Domain;
using Library.Host;
using Library.Projections;
using Microsoft.Extensions.DependencyInjection;

namespace Library.Domain.Tests;

public sealed class LibraryBookTests
{
    [Fact]
    public async Task borrow_emits_event_and_updates_catalog_view()
    {
        await using var ctx = BddTestContext.CreateInMemory(typeof(BookAdded), typeof(BookBorrowed), typeof(BookReturned));
        var bookId = Guid.NewGuid();
        var projector = new LibraryProjector();

        await AggregateSpec
            .For<Book>(ctx, bookId)
            .Given(new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "A Tale of Two Cities", "ABCD"))
            .When(new BorrowBookCommand(Guid.NewGuid(), bookId, "Jeremy"))
            .ThenEmittedEvent<BookBorrowed>(e => e.BookId == bookId && e.MemberName == "Jeremy")
            .AndExpectedVersion(2)
            .AndView(r => r.Register(projector.Apply))
            .AndAssert(result =>
            {
                Assert.True(result.Aggregate.State.OnLoan);
                Assert.Equal("Jeremy", result.Aggregate.State.BorrowedBy);

                var view = projector.GetBook(bookId);
                Assert.NotNull(view);
                Assert.Equal("A Tale of Two Cities", view.Title);
                Assert.True(view.OnLoan);
                Assert.Single(projector.BorrowedBooks);
            })
            .RunAsync();
    }

    [Fact]
    public async Task return_clears_loan_and_removes_borrowed_view()
    {
        await using var ctx = BddTestContext.CreateInMemory(typeof(BookAdded), typeof(BookBorrowed), typeof(BookReturned));
        var bookId = Guid.NewGuid();
        var projector = new LibraryProjector();

        await AggregateSpec
            .For<Book>(ctx, bookId)
            .Given(
                new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "A Tale of Two Cities", "ABCD"),
                new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, bookId, "Jeremy"))
            .When(new ReturnBookCommand(Guid.NewGuid(), bookId))
            .ThenEmittedEvent<BookReturned>(e => e.BookId == bookId)
            .AndExpectedVersion(3)
            .AndView(r => r.Register(projector.Apply))
            .AndAssert(result =>
            {
                Assert.False(result.Aggregate.State.OnLoan);
                Assert.Null(result.Aggregate.State.BorrowedBy);

                var view = projector.GetBook(bookId);
                Assert.NotNull(view);
                Assert.False(view.OnLoan);
                Assert.Empty(projector.BorrowedBooks);
            })
            .RunAsync();
    }

    [Fact]
    public async Task cannot_borrow_when_already_on_loan()
    {
        await using var ctx = BddTestContext.CreateInMemory(typeof(BookAdded), typeof(BookBorrowed), typeof(BookReturned));
        var bookId = Guid.NewGuid();

        await AggregateSpec
            .For<Book>(ctx, bookId)
            .Given(
                new BookAdded(Guid.NewGuid(), DateTime.UtcNow, bookId, "Pragmatic Programmer", "978-0135957059"),
                new BookBorrowed(Guid.NewGuid(), DateTime.UtcNow, bookId, "Jane Doe"))
            .When(new BorrowBookCommand(Guid.NewGuid(), bookId, "Someone Else"))
            .ThenThrows<DomainException>()
            .AndAssert(result =>
                Assert.Equal("Book is already on loan.", result.Exception!.Message))
            .RunAsync();
    }

    [Fact]
    public void add_in_memory_library_registers()
    {
        var services = new ServiceCollection();
        LibraryHost.AddInMemoryLibrary(services);
        using var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetService<IAggregateRepository>());
        Assert.NotNull(sp.GetService<LawnDart.ICommandHandler<BorrowBookCommand>>());
    }
}
