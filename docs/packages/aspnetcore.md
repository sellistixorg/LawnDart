# LawnDart.AspNetCore

HTTP command mapping. Does not reference the auth package.

```csharp
services.AddLawnDartHttpCommands(typeof(CreateOrderCommand).Assembly);
app.MapLawnDartCommands();
```

See [HTTP_COMMANDS.md](../HTTP_COMMANDS.md).
