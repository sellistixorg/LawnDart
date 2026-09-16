# LawnDart.Testing

Given / when / then specs and messaging harnesses. `BddTestContext.CreateInMemory`
is the only factory — no Docker.

Take it for domain tests. SQL Server integration tests live in the
`*.SqlServer.Tests` projects and are tagged `Category=Integration`.

## Registration

No host registration. Reference the package from the test project:

Excerpted from `samples/Library.Domain.Tests/LibraryBookTests.cs`:

```csharp
await using var ctx = BddTestContext.CreateInMemory();
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

Also ships `DcbSpec`, `ReactorTestHarness<TReactor, TEvent>`, and
`WaitForAsync` for eventual-consistency assertions.

## Related

- [Package map](README.md)
- [BDD testing](../testing/BDD_TESTING.md)
- [Testing your aggregate](../learning-path/03-testing-your-aggregate.md)
- [Testing EDA](../learning-path/07-testing-eda.md)
