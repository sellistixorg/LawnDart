---
name: lawndart-aspnet-hosting
description: Map LawnDart HTTP commands and optional HTTP authorization. Use AddLawnDartHttpCommands and MapLawnDartCommands. Auth package is additive.
---

# ASP.NET hosting

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
