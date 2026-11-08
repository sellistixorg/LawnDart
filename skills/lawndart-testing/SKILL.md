---
name: lawndart-testing
description: Write LawnDart given / when / then specs with AggregateSpec and DcbSpec. Use when proving a slice against UseInMemory.
---

# Testing

Package `LawnDart.Testing`, namespace `LawnDart.Testing.Bdd`. Events in
`Given` / `When` need `[EventTypeName]`. Pass those types to
`BddTestContext.CreateInMemory(...)`.

Domain rules throw `DomainException`. Assert with
`ThenThrows<DomainException>()`. Wrapping `RunAsync` in
`Assert.ThrowsAsync<InvalidOperationException>` is the wrong check.

## Aggregate

Excerpt from `samples/Library.Domain.Tests/LibraryBookTests.cs`
(`cannot_borrow_when_already_on_loan`):

```csharp
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
```

`AndView` replays Given + When through a projector `Apply` callback. See
`borrow_emits_event_and_updates_catalog_view` in the same file.

## DCB

`DcbSpec` uses the same verbs. `For` takes the decision query. `Given`
takes the tags of that one event. Excerpt from
`samples/Library.Dcb.Domain.Tests/LibraryBookLoanTests.cs`
(`cannot_borrow_when_already_on_loan`):

```csharp
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
```

See `docs/testing/BDD_TESTING.md`.
