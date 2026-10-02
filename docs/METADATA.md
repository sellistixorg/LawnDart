# Metadata

`AddLawnDart` registers `DefaultMetadataProvider`. `HandleCommandAsync` (aggregate
or DCB) captures a `CommandMetadata` if you omit one, then enriches each
pending event before append. HTTP and `ICommandDispatcher` publish inbound
`MessageContext` on `AmbientMessageContext` first, so the default provider
sees that envelope.

## Three carriers

| Type | Role |
|---|---|
| `MessageContext` | Inbound transport (HTTP, messaging). Correlation, causation, tenant, user, and W3C headers. |
| `CommandMetadata` | Capture at command time. Built by `IMetadataProvider.CaptureCommandMetadata`, or passed into `HandleCommandAsync`. |
| `EventMetadata` | Enrich on append, then the typed session snapshots it onto the log frame. |

`CaptureCommandMetadata(object? context)` reads `context` as a
`MessageContext`, or `AmbientMessageContext.Current` when `context` is
null. Pass a `MessageContext`. The default provider does not read
`HttpContext`.

## What the default fills

`DefaultMetadataProvider.CaptureCommandMetadata` sets:

- `Timestamp` (`DateTime.UtcNow`)
- `CorrelationId` (`MessageContext.CorrelationId`, else the current W3C
  trace id, else a new GUID)
- `CausationId` (`MessageContext.CausationId`)
- `TenantId` (`MessageContext.TenantId`, else `ITenantContextProvider`)
- `UserId` (`MessageContext.UserId`)
- `TraceId` / `SpanId` (`Activity.Current` when it is W3C, else
  `traceparent` on `MessageContext.Headers`)

`HandleCommandAsync` then runs `CausationId ??= command.Id.ToString()`.
HTTP leaves inbound causation unset, so the command id is the usual value.

`EnrichEventMetadata` copies those fields onto `EventMetadata`, plus
`EventId` and `Timestamp` from the `IEvent`. Event business time is the
event's `Timestamp`, not `UtcNow`. If `CausationId` is still unset at
enrich (a direct `EnrichEventMetadata` call), it falls back to
`CorrelationId`.

Read the stored envelope on `SequencedEvent.Metadata` after
`ReadStreamAsync`.

## HTTP headers

`MapLawnDartCommands` builds the inbound `MessageContext` from:

| Header or claim | Field |
|---|---|
| `X-Tenant-Id` | `TenantId` |
| `sub` | `UserId` |
| `traceparent` / `tracestate` | `Headers`; `CorrelationId` is the parent trace id when `traceparent` parses |
| `Idempotency-Key` | `MessageId`; if the body `Id` is empty and the header is a GUID, that GUID becomes `ICommand.Id` |

`TransportType` is `HTTP`. Routes and status codes are on
[HTTP commands](HTTP_COMMANDS.md).

## Extra fields

These exist on `CommandMetadata` / `EventMetadata` and stay empty on the
default capture path: `UserName`, `AccountId`, `IpAddress`, `UserAgent`,
`AuthorizedBy`, `AuthorizedAt`, `AuthorizationPolicies`, `Custom`.

Fill them by passing `CommandMetadata` into `HandleCommandAsync`, or by
registering `IMetadataProvider`.

```csharp
await repo.HandleCommandAsync(
    counter,
    new IncrementCommand(Guid.NewGuid(), id),
    new CommandMetadata { UserId = "alice", UserName = "Alice" });
```

When `LawnDartOptions.EnableAuthorization` is on and the check succeeds,
`HandleCommandAsync` writes `AuthorizedAt`. The aggregate path also sets
`AuthorizedBy` to `UserId`. The DCB path writes `AuthorizationPolicies`
from the auth result (empty on success).

## Schema name and version

The typed session (`EventSession`) stamps `SchemaVersion` from the current
CLR type onto the log frame and onto a copy of `EventMetadata`.
`SchemaName` is the catalog token. Hydrate copies `SchemaVersion` and
`CommitTimestamp` from the frame. The frame is authority.

See [Event schema versioning](EVENT_SCHEMA_VERSIONING.md).

## Replace the default provider

`AddMetadataProvider<T>` is `AddSingleton`, not `TryAdd`. Call it after
`AddLawnDart`. Repositories resolve `IMetadataProvider` with
`GetRequiredService`, which is the last registration.

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddMetadataProvider<HostMetadataProvider>();
```

```csharp
public sealed class HostMetadataProvider : IMetadataProvider
{
    private readonly DefaultMetadataProvider _inner = new();

    public CommandMetadata CaptureCommandMetadata(object? context = null)
    {
        var captured = _inner.CaptureCommandMetadata(context);
        captured.AccountId = "ops";
        return captured;
    }

    public EventMetadata EnrichEventMetadata(
        EventMetadata baseMetadata,
        CommandMetadata commandMetadata,
        IEvent @event)
        => _inner.EnrichEventMetadata(baseMetadata, commandMetadata, @event);
}
```

Swap tenant source with `AddTenantContextProvider<T>` when the host is
multi-tenant. Registration names are on the
[extension method index](EXTENSION_METHOD_INDEX.md).

## Three clocks

Keep these apart. Definitions stay in the [glossary](GLOSSARY.md#event-clocks).

- **Business time:** `IEvent.Timestamp` and `EventMetadata.Timestamp` after
  enrich. Time-travel (`toTimestamp`) uses this.
- **Commit time:** `RecordedEvent.CommitTimestamp`, mirrored onto
  `EventMetadata.CommitTimestamp` on hydrate. Default enrich leaves it
  null.
- **Trace:** `TraceId` / `SpanId` (W3C hex). Not a third `DateTime`.

## Related

- [DI Grammar](DI_GRAMMAR.md)
- [HTTP commands](HTTP_COMMANDS.md)
- [Event schema versioning](EVENT_SCHEMA_VERSIONING.md)
- [Glossary](GLOSSARY.md)
