---
name: lawndart-projection-authoring
description: Write LawnDart Lightweight projection classes, view DTOs, and view keys. Use when adding a read model.
---

# Projection authoring

Define a view DTO and a class that extends `ProjectionBase<TView>` with a
**scope** attribute (`[SingleStreamProjection]`, `[GlobalProjection]`,
`[DcbProjection]`, `[MultiStreamProjection]`). `[ProjectionEndpoint]` maps
an optional HTTP GET. Use `LawnDart.Projections.Lightweight` /
`LawnDart.Projections.Sdk`.

A view that spans stream types uses `[MultiStreamProjection]` and implements
`IMultiStreamEntityResolver` on the handler (`GetEntityId`).

Do not implement `IProjector<TState>`. Lightweight does not call it.

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
