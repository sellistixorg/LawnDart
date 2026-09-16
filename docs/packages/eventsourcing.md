# LawnDart.EventSourcing

Runtime plus the in-memory **implementation** of `IEventLog` / `IEventStore`.

The `IEventStore` **interface** is in core `LawnDart`. This package provides
`InMemoryEventStore`, `AggregateRepository`, `DcbRepository`, JSON
serialization, and outbox processing. The InMemory → SQL `Category=Contract`
suite includes the event-log thesis: append/read frames without registered
CLR types.

Take it for local work, tests, and Academy. Swap the backend later without
changing domain types.

## Registration

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .UseInMemory()
    .WithEventTypes(typeof(SomeEvent).Assembly);
```

Registers keyed `IEventStore`, `IAggregateRepository`, `IDcbRepository`,
`ISnapshotStore` / `IDcbSnapshotStore`, and `IOutboxWriter` for that context
name. The `"default"` context also gets unkeyed aliases, so
`GetRequiredService<IAggregateRepository>()` works without a key.

Append stores the `[EventTypeName]` token, not CLR `FullName`.
InMemory serializes on append and hydrates a new instance on read — it is not
an object heap. The session codec is `IEventSerializer`
(`ReadOnlyMemory<byte>`). Excerpt from
`src/LawnDart.EventSourcing/Serialization/JsonEventSerializer.cs`:

```csharp
public ReadOnlyMemory<byte> Serialize(object obj, Type type)
{
    return JsonSerializer.SerializeToUtf8Bytes(obj, type, _options);
}

public object Deserialize(ReadOnlyMemory<byte> data, Type type)
{
    return JsonSerializer.Deserialize(data.Span, type, _options)
        ?? throw new InvalidOperationException($"Failed to deserialize {type.Name}");
}
```
`ContextAwareCommandDispatcher` publishes inbound `MessageContext` (and
continues `traceparent`) before `HandleAsync`. `HandleCommandAsync` sets
envelope `CausationId` to the command id when the caller left it unset.

`IAggregateRepository` loads by `Guid` (`{type}:{id}` / `{tenant}:{type}:{id}`)
or by `string streamId` when the stream is a custom identity. Prefer
`GetOrCreateAsync<T>(streamId)` over setting `StreamId` yourself.

`WithCommandHandlers` scans handler assemblies and registers
`ICommandDispatcher` (Core). `UseInMemory` registers the same dispatcher.

Snapshot write durability (synchronous capture, wait-free enqueue, drop-oldest,
health) and replace-in-place retention: [SNAPSHOTS.md](../SNAPSHOTS.md). Custom stores use
`EventSourcingRepositories` and `AddSnapshotWriteInfrastructure` so snapshot
writes still enqueue.

## Portable store contract

This package implements `IEventLog` and the typed `IEventStore` adapter
(which includes `IStreamRegistry`) plus `IEventStoreSubscriptions`.
Application signatures live in Core and must not change. Third-party
stores implement the log. The shipped session still exposes:

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
