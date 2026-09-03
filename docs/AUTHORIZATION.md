# Authorization

Core contracts live in `LawnDart`. The HTTP adapter is
`LawnDart.Authorization.AspNetCore`.

## Pieces

| Type | Role |
|---|---|
| `AuthorizationContext` | Roles, permission claims, entitlements |
| `IAuthorizationContextProvider` | How the host fills that context |
| `IAuthorizationProvider` | How a permission/entitlement/policy is decided |
| `AuthorizationService` | Runs attributes on a command |
| `[RequiresPermission]` / `[RequiresEntitlement]` / `[RequiresPolicy]` | Declarative checks |

## HTTP host

```csharp
builder.Services.AddLawnDart(o => o.EnableAuthorization = true);
builder.Services.AddLawnDartAuthorization();
builder.Services.AddHttpAuthorizationContext();
builder.Services.AddLawnDartHttpCommands(typeof(CreateOrderCommand).Assembly);

app.UseAuthentication();
app.UseAuthorization();
app.MapLawnDartCommands();
```

`HttpAuthorizationContextProvider` reads JWT / user claims. Academy maps
`Instructor` and `Student` roles to demo permissions in
`AcademyAuthorizationProvider`.

## Soft dependency

`LawnDart.AspNetCore` does **not** reference the auth package. If no context
provider is registered, it installs a missing-provider stub so commands without
attributes still map. Adding the auth package + `AddHttpAuthorizationContext`
enables claim checks.

## Repositories

`IAggregateRepository.HandleCommandAsync` and `IDcbRepository.HandleCommandAsync`
call `AuthorizationService` when it is registered and the command has attributes.
