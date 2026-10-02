# Authorization

Core contracts live in `LawnDart`. The HTTP adapter is
`LawnDart.Authorization.AspNetCore`.

Turn authorization on with `LawnDartOptions.EnableAuthorization` for
repository checks, command attributes for the rules, and
`IAuthorizationContextProvider` for the caller. HTTP failed auth is
`403 Forbidden`. Routes and the other status codes:
[HTTP commands](HTTP_COMMANDS.md).

## Pieces

| Type | Role |
|---|---|
| `AuthorizationContext` | Roles, permission claims, entitlements |
| `IAuthorizationContextProvider` | How the host fills that context |
| `IAuthorizationProvider` | How a permission, entitlement, or policy is decided |
| `AuthorizationService` | Runs attributes on a command |
| `[RequiresPermission]` / `[RequiresEntitlement]` / `[RequiresPolicy]` | Declarative checks |

## HTTP host

```csharp
builder.Services.AddLawnDart(o => o.EnableAuthorization = true);
builder.Services.AddLawnDartAuthorization();
builder.Services.AddHttpAuthorizationContext();
var ctx = builder.Services.AddBoundedContext("default").UseInMemory();
ctx.WithCommandHandlers<CreateOrderCommandHandler>();
builder.Services.AddLawnDartHttpCommands(typeof(CreateOrderCommand).Assembly);

app.UseAuthentication();
app.UseAuthorization();
app.MapLawnDartCommands();
```

`HttpAuthorizationContextProvider` reads JWT / user claims. Academy maps
`Instructor` and `Student` roles to demo permissions in
`AcademyAuthorizationProvider`.

When a command has `[RequiresPermission]`, `[RequiresEntitlement]`, or
`[RequiresPolicy]`, the endpoint calls `AuthorizationService.AuthorizeCommandAsync`
before the handler. A failed check returns `403 Forbidden`
(`ProblemDetails`). Commands with no attributes skip that check.

## Soft dependency

`LawnDart.AspNetCore` does not reference the auth package. If no context
provider is registered, it installs a missing-provider stub so commands
without attributes still map. Adding the auth package and
`AddHttpAuthorizationContext` enables claim checks. Attributed commands
fail until that provider is registered.

## Repositories

When `EnableAuthorization` is true and `AuthorizationService` is
registered, `IAggregateRepository.HandleCommandAsync` and
`IDcbRepository.HandleCommandAsync` call `AuthorizeCommandAsync`. A
failed check throws `UnauthorizedAccessException`.
