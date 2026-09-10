# LawnDart.EventSourcing

Runtime plus the in-memory **implementation** of `IEventStore`.

The `IEventStore` **interface** is in core `LawnDart`. This package provides
`InMemoryEventStore`, `AggregateRepository`, `DcbRepository`, JSON
serialization, and outbox processing.

Take it for local work, tests, and Academy. Swap the backend later without
changing domain types.

## Registration

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .UseInMemory()
    .WithEventTypes(typeof(SomeEvent).Assembly);
```

Registers keyed `IEventStore`, `IAggregateRepository`, and `IDcbRepository`
for that context name. The `"default"` context also gets unkeyed aliases, so
`GetRequiredService<IAggregateRepository>()` works without a key.

Append stores the `[EventTypeName]` token, not CLR `FullName`.
`ContextAwareCommandDispatcher` publishes inbound `MessageContext` (and
continues `traceparent`) before `HandleAsync`. `HandleCommandAsync` sets
envelope `CausationId` to the command id when the caller left it unset.

`IAggregateRepository` loads by `Guid` (`{type}:{id}` / `{tenant}:{type}:{id}`)
or by `string streamId` when the stream is a custom identity. Prefer
`GetOrCreateAsync<T>(streamId)` over setting `StreamId` yourself.

`WithCommandHandlers` scans handler assemblies and registers
`ICommandDispatcher` (Core). `UseInMemory` registers the same dispatcher.

## Portable store contract

This package implements `IEventStore` (which includes `IStreamRegistry`)
and `IEventStoreSubscriptions`. Those signatures live in Core and must
not change. Implementers keep:

- **`IEventStore`:** `ReadStreamAsync`, `ReadStreamEnumerableAsync`,
  `ReadByQueryAsync`, `ReadByQueryStreamAsync`, stream `AppendAsync`,
  DCB `AppendAsync`, `GetCurrentSequenceAsync`, `GetMaxSequencePositionAsync`
- **`IStreamRegistry`:** `GetStreamAsync`, `GetStreamsByAggregateTypeAsync`,
  `GetStreamsByTagAsync`, `EnumerateStreamIdsAsync`,
  `GetStreamsUpdatedAfterAsync`, `GetStreamCountAsync`
- **`IEventStoreSubscriptions`:** `Subscribe`

Full signatures: [core.md](core.md#portable-store-contract) and
[BACKEND_SELECTION.md](../BACKEND_SELECTION.md).

## Related

- [Package map](README.md)
- [Quickstart](../QUICKSTART.md)
- [Backend selection](../BACKEND_SELECTION.md)
- [SqlServer package](eventsourcing-sqlserver.md)
