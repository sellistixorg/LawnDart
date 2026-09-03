# LawnDart.AspNetCore

Maps command handlers to HTTP POST endpoints.

```csharp
services.AddLawnDartHttpCommands(typeof(SomeCommand).Assembly);
app.MapLawnDartCommands();
```

Authorization is additive: add `LawnDart.Authorization.AspNetCore` and call
`AddLawnDartAuthorization()` + `AddHttpAuthorizationContext()`.
