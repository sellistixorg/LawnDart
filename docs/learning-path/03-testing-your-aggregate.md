# Step 3 — Testing your aggregate

**Previous:** [First aggregate](02-first-aggregate.md) · **Next:** [Reading state](04-reading-state.md)

Use `LawnDart.Testing` against InMemory. No Docker. Events from
[step 2](02-first-aggregate.md) already declare `[EventTypeName]`. Pass
those types to `CreateInMemory` so the spec has a scoped catalog.

Excerpted from `samples/Library.Domain.Tests/LibraryBookTests.cs`.

```csharp
await using var ctx = BddTestContext.CreateInMemory(
    typeof(BookAdded), typeof(BookBorrowed), typeof(BookReturned));
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

Rule violations throw `DomainException`. `ThenThrows<T>()` is the
domain-rule assertion. Spec-internal failures throw
`BddSpecAssertionException`, which does not derive from
`InvalidOperationException`. Do not wrap `RunAsync()` in
`Assert.ThrowsAsync<InvalidOperationException>`.

Full guide: [BDD_TESTING.md](../testing/BDD_TESTING.md).

SQL Server tests are tagged `Category=Integration` and need Docker. Local
unit runs use `--filter Category!=Integration`. CI runs both.
