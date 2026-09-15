---
name: lawndart-projection-authoring
description: Write LawnDart Lightweight projection classes, view DTOs, and view keys. Use when adding a read model.
---

# Projection authoring

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs).
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization.
3. **Load** by `string streamId` when the stream is not `{type}:{guid}`.
4. **Projections:** `ProjectionBase<TView>` plus attributes. Multi-stream views implement `IMultiStreamEntityResolver`. Do not implement `IProjector` unless you are writing your own fold.
5. **Stores:** `UseInMemory` / `UseSqlServer` on `AddBoundedContext(name)`.

Define a view DTO and a class that extends `ProjectionBase<TView>` with a
scope attribute (`[SingleStreamProjection]`, `[GlobalProjection]`,
`[DcbProjection]`, `[ProjectionEndpoint]`). Use
`LawnDart.Projections.Lightweight` / `LawnDart.Projections.Sdk`.
Do not implement `IProjector` — it is experimental and Lightweight does
not call it. A view that spans stream types implements
`IMultiStreamEntityResolver` on the handler (the Flywheel multi-stream
hook).

Excerpt from `samples/Library.Host/LibraryCatalogProjection.cs`.
`BookCatalogView` is in `samples/Library.Domain/Projections/LibraryProjector.cs`.

```csharp
[SingleStreamProjection("BookCatalog", streamType: "Book")]
public sealed class LibraryCatalogProjection : ProjectionBase<BookCatalogView>
{
    public void Handle(BookAdded e)
    {
        State.BookId = e.BookId;
        State.Title = e.Title;
        State.Isbn = e.Isbn;
        State.OnLoan = false;
        State.BorrowedBy = null;
    }

    public void Handle(BookBorrowed e)
    {
        State.OnLoan = true;
        State.BorrowedBy = e.MemberName;
    }

    public void Handle(BookReturned e)
    {
        State.OnLoan = false;
        State.BorrowedBy = null;
    }
}
```

The beginner GWT path uses `LibraryProjector.Apply` with `AndView` (no
Lightweight host). Lightweight is the production host for the same view
shape.

Keep projectors deterministic. Do not call the event store from `Handle`.
Put auth on query endpoints with `ProjectionEndpointAttribute` when the view
is tenant-scoped.

See `demos/LawnDart.Demo.Academy.WebApi/Projections` for a larger host.
