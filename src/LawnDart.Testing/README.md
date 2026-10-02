# LawnDart.Testing

Given / when / then specs and messaging harnesses. Start with
`BddTestContext.CreateInMemory` and pass the event types the spec uses.

Excerpted from `samples/Library.Domain.Tests/LibraryBookTests.cs`
(`cannot_borrow_when_already_on_loan`; method signature omitted):

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

See [BDD testing](https://github.com/sellistixorg/LawnDart/blob/main/docs/testing/BDD_TESTING.md).
