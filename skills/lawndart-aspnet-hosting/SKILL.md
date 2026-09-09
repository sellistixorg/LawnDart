---
name: lawndart-aspnet-hosting
description: Map LawnDart HTTP commands and optional HTTP authorization. Use AddLawnDartHttpCommands and MapLawnDartCommands. Auth package is additive.
---

# ASP.NET hosting

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` — `AddLawnDartHttpCommands` / `MapLawnDartCommands`.
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. The handler calls `HandleCommandAsync` for persistence + authorization.
3. **Load** by `string streamId` when the stream is not `{type}:{guid}`.
4. **Projections:** `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`. Map GETs with `MapProjectionQueries(name)`.
5. **Stores:** `UseInMemory` / `UseSqlServer` on `AddBoundedContext(name)`.

```csharp
builder.Services.AddLawnDartHttpCommands(typeof(RegisterStudentCommand).Assembly);

// Optional claims → AuthorizationContext
builder.Services.AddHttpAuthorizationContext();

app.UseAuthentication();
app.UseAuthorization();
app.MapLawnDartCommands();
app.MapProjectionQueries("default");
```

`LawnDart.AspNetCore` does not reference `LawnDart.Authorization.AspNetCore`.
Commands without `[RequiresPermission]` run without a claims provider.

Routes: strip `Command`, kebab-case, last namespace segment as group,
default prefix `api`.

See `docs/HTTP_COMMANDS.md` and `demos/LawnDart.Demo.Academy.WebApi`.
