# HTTP commands

`LawnDart.AspNetCore` maps `ICommandHandler<TCommand>` to POST endpoints.
Register handlers with `WithCommandHandlers<TMarker>()` on the bounded
context. `AddLawnDartHttpCommands` records assemblies for routing and
authorization. `MapLawnDartCommands` maps those handlers to POST.

Host grammar: [DI Grammar](DI_GRAMMAR.md).

HTTP endpoints call `ICommandHandler<T>` directly. Reactors and task
processors go through `ICommandDispatcher` (Core, `LawnDart.Messaging`
namespace), registered by `UseInMemory` / `WithCommandHandlers`.

## Register

```csharp
[RequiresPermission("Orders.Create")]
public record CreateOrderCommand(Guid Id, string ProductCode, int Quantity) : ICommand;

public sealed class CreateOrderCommandHandler : ICommandHandler<CreateOrderCommand>
{
    public Task HandleAsync(CreateOrderCommand command, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}

var ctx = builder.Services.AddBoundedContext("default").UseInMemory();
ctx.WithCommandHandlers<CreateOrderCommandHandler>();
builder.Services.AddLawnDartHttpCommands(typeof(CreateOrderCommand).Assembly);
app.MapLawnDartCommands();
```

Auth package is optional. Without `LawnDart.Authorization.AspNetCore`,
commands that have no auth attributes still run. Commands that declare
attributes need an `IAuthorizationContextProvider` (HTTP claims via
`AddHttpAuthorizationContext`). See [Authorization](AUTHORIZATION.md).

## Route convention

| Command type | Typical route |
|---|---|
| `CreateOrderCommand` in `Orders` | `POST /api/orders/create-order` |
| `RegisterStudentCommand` in `WebApi` | `POST /api/web-api/register-student` |

Rules:

- Strip the `Command` suffix.
- PascalCase to kebab-case.
- Last meaningful namespace segment is the group (`Commands` is ignored).
- Default prefix is `api`. Override with `MapLawnDartCommands(o => o.RoutePrefix = "v1")`.

## Headers and command identity

`MapLawnDartCommands` builds inbound `MessageContext` from:

| Header or claim | Field |
|---|---|
| `X-Tenant-Id` | `TenantId` |
| `sub` | `UserId` |
| `traceparent` / `tracestate` | `Headers`; `CorrelationId` is the parent trace id when `traceparent` parses |
| `Idempotency-Key` | `MessageId`; if the body `Id` is empty and the header is a GUID, that GUID becomes `ICommand.Id` |

`TransportType` is `HTTP`. Envelope `TraceId` / `SpanId` and
`CorrelationId` come from the W3C trace, not `HttpContext.TraceIdentifier`.
The body `Id` still wins when present.

The endpoint publishes that context on `AmbientMessageContext` before
`HandleAsync`. `HandleCommandAsync` then captures correlation, causation,
and tenant from it, and sets `CausationId` to the command id when the
caller left it unset. Default fill: [Metadata](METADATA.md).

## Status codes

`CommandEndpointRegistrar` returns:

| Outcome | Status | Body |
|---|---|---|
| Handler succeeds | `202 Accepted` | empty |
| Auth attributes fail | `403 Forbidden` | `ProblemDetails`, title `Forbidden` |
| `ConcurrencyException` | `409 Conflict` | `ProblemDetails`, title `Conflict` |
| `DomainException` | `422 Unprocessable Entity` | `ProblemDetails`, title `ex.Title` (default `Domain rule violated`) |

Commands with no auth attributes skip the permission check.

See Academy WebApi: `demos/LawnDart.Demo.Academy.WebApi`.
