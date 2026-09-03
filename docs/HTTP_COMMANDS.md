# HTTP commands

`LawnDart.AspNetCore` scans assemblies for `ICommandHandler<TCommand>` and maps
POST endpoints. Authorization attributes on the command are optional.

## Workflow

```csharp
[RequiresPermission("Orders.Create")]
public record CreateOrderCommand(Guid Id, string ProductCode, int Quantity) : ICommand;

public sealed class CreateOrderCommandHandler : ICommandHandler<CreateOrderCommand>
{
    public Task HandleAsync(CreateOrderCommand command, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}

builder.Services.AddLawnDartHttpCommands(typeof(CreateOrderCommand).Assembly);
app.MapLawnDartCommands();
```

Auth package is optional. Without `LawnDart.Authorization.AspNetCore`, commands
that have no auth attributes still run. Commands that declare attributes need
an `IAuthorizationContextProvider` (HTTP claims via `AddHttpAuthorizationContext`).

## Route convention

| Command type | Typical route |
|---|---|
| `CreateOrderCommand` in `Orders` | `POST /api/orders/create-order` |
| `RegisterStudentCommand` in `WebApi` | `POST /api/web-api/register-student` |

Rules:

- Strip the `Command` suffix.
- PascalCase → kebab-case.
- Last meaningful namespace segment is the group (`Commands` is ignored).
- Default prefix is `api`. Override with `MapLawnDartCommands(o => o.RoutePrefix = "v1")`.

## Status codes

- Authorized / no attributes → `202 Accepted` after the handler succeeds.
- Failed auth → `403 Forbidden` (`ProblemDetails`).
- Domain failures surface as the handler's exception (map to problem details in
  your host if you want a custom shape).

See Academy WebApi: `demos/LawnDart.Demo.Academy.WebApi`.
