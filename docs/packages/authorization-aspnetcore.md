# LawnDart.Authorization.AspNetCore

HTTP claims → `AuthorizationContext`.

Take it when `MapLawnDartCommands` should enforce `[Authorize]`-style
attributes from the incoming user. Core auth contracts and
`AddLawnDartAuthorization` stay in `LawnDart`; this package is the HTTP
adapter only.

`LawnDart.AspNetCore` maps commands without this package.

## Registration

```csharp
services.AddLawnDartAuthorization();
services.AddHttpAuthorizationContext();
```

## Related

- [Package map](README.md)
- [Authorization](../AUTHORIZATION.md)
- [AspNetCore](aspnetcore.md)
