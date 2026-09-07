# LawnDart.AspNetCore

HTTP command mapping. Discovers command handlers and maps them to POST
endpoints.

Take it when a Web API should accept commands. This package does **not**
reference the auth package — claims mapping is additive.

## Registration

```csharp
services.AddLawnDartHttpCommands(typeof(CreateOrderCommand).Assembly);
app.MapLawnDartCommands();
```

## Related

- [Package map](README.md)
- [HTTP commands](../HTTP_COMMANDS.md)
- [Authorization.AspNetCore](authorization-aspnetcore.md)
- [Production shape](../learning-path/08-production.md)
