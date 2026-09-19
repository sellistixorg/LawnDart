---
name: lawndart-aspnet-hosting
description: Map LawnDart HTTP commands and optional HTTP authorization. Register handlers with WithCommandHandlers, then AddLawnDartHttpCommands and MapLawnDartCommands. Use when exposing commands over HTTP.
---

# ASP.NET hosting

Register handlers with `WithCommandHandlers<TMarker>()`, then
`AddLawnDartHttpCommands` / `MapLawnDartCommands` for routing.
Map GETs with `MapProjectionQueries(name)`.

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddInMemoryLibrary` + `MapLibraryHttp`):

```csharp
ctx.WithCommandHandlers<BorrowBookHandler>();
services.AddLawnDartHttpCommands(typeof(BorrowBookHandler).Assembly);
services.AddHttpAuthorizationContext();
app.MapLawnDartCommands();
app.MapProjectionQueries("default");
```

`LawnDart.AspNetCore` does not reference `LawnDart.Authorization.AspNetCore`.
Commands without `[RequiresPermission]` run without a claims provider.

For repository checks, set `EnableAuthorization` on `AddLawnDart` and call
`AddLawnDartAuthorization`. Academy WebApi is the runnable auth host.

Routes: strip `Command`, kebab-case, last meaningful namespace segment as
group (`Commands` and `Patterns` are skipped), default prefix `api`.

Status: handler success `202`, failed auth `403`, `ConcurrencyException`
`409`, `DomainException` `422`. See `docs/HTTP_COMMANDS.md`.
