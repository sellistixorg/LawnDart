# BDD testing

`LawnDart.Testing` runs Given / When / Then specs against `UseInMemory()`.

Events used in `Given` / `When` must declare `[EventTypeName]`. In-memory GWT
does not call `WithEventTypes`; the attribute is enough for writes. After
`RunAsync`, `result.EmittedSequencedEvents` carries envelope `CausationId`
(the command id when the caller left it unset) and a stable `CorrelationId`.
Those fields live on the envelope, not on `ICommand` / `IEvent`.

## Package

- `LawnDart.Testing`
- Namespace `LawnDart.Testing.Bdd`
- Types: `BddTestContext`, `AggregateSpec`, `DcbSpec`, `BddProjectionRunner`,
  `BddSpecAssertionException`

## Aggregate spec

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
    .ThenThrows<InvalidOperationException>()
    .AndAssert(result =>
        Assert.Equal("Book is already on loan.", result.Exception!.Message))
    .RunAsync();
```

## DCB spec

`DcbSpec` uses the same Given / When / Then verbs with tags. The reference
slice has no DCB entity. The compiled DCB example lives in
`tests/LawnDart.Testing.Tests/Bdd/InMemoryGwtTests.cs`.

## AndView

`AndView` replays the whole stream (Given + When) through a projector
`Apply` callback. The callback does not need `LawnDart.Projections.Lightweight`.
Excerpted from `samples/Library.Domain.Tests/LibraryBookTests.cs`:

```csharp
await using var ctx = BddTestContext.CreateInMemory();
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
```

## Test project

Reference `LawnDart`, `LawnDart.EventSourcing`, `LawnDart.Testing`, plus your
domain project. Use xUnit. Canonical proofs:
`samples/Library.Domain.Tests/LibraryBookTests.cs` (reference slice) and
`tests/LawnDart.Testing.Tests/Bdd/InMemoryGwtTests.cs`.

These specs match the Eventhesis GWT widget — see [EVENTHESIS.md](../EVENTHESIS.md).

## Exception contract

Spec-internal failures — missing `When`, a `Then*` mismatch, an unexpected
exception from `When`, or a broken store probe — throw
`BddSpecAssertionException`. That type does **not** derive from
`InvalidOperationException`.

Do not wrap `RunAsync()` in
`Assert.ThrowsAsync<InvalidOperationException>`. A failed spec assertion
would have passed that test. Use `ThenThrows<T>()` for a domain rule:

- `ThenThrows<T>()` passes when the domain throws `T` or a type derived from `T`.
- A mismatch throws `BddSpecAssertionException` and names both the expected
  and actual types.
- Domain code may still throw `InvalidOperationException` (or
  `DomainException`, which derives from it). Those remain the When
  exception; only the spec runner uses `BddSpecAssertionException`.

A store that does not implement `GetCurrentSequenceAsync` or
`ReadByQueryAsync` throws `NotSupportedException`. The spec treats that as
a missing capability: sequence baseline becomes `-1`, and the appended-stream
set is empty. Any other exception from those probes is a broken store and
surfaces as `BddSpecAssertionException`.
