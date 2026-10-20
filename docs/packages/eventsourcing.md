# LawnDart.EventSourcing

Runtime plus the in-memory **implementation** of `IEventLog` / `IEventStore`.

The `IEventStore` **interface** is in core `LawnDart`. This package provides
`InMemoryEventStore`, `AggregateRepository`, `DcbRepository`, JSON
serialization, and outbox processing. The shared `Category=Contract` suite
runs this store through the same checks as SQL Server: the event log, appends,
the dynamic consistency boundary, tags, the stream registry, subscriptions,
snapshots, the outbox writer, the inbox, checkpoints, and views. A check that
needs a SQL Server hook is skipped here with a reason.

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

`InMemoryEventStoreOptions.RequireTenantId` defaults to true. `UseInMemory` copies `RequireTenantId` and `ValidateNames` onto the store. An append rejects a missing tenant id, an empty tenant id, and `default`. Set `RequireTenantId` to false to append without a tenant id.

Append stores the `[EventTypeName]` event name. The .NET type's `FullName`
is not stored. InMemory serializes on append and returns a new instance on
read. The store keeps records, not a live object heap. The session payload
format is `IEventSerializer`
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
`WithTagFormat<T>()` registers the context `ITagFormat`. `AddLawnDart`
already registered `LowercaseTagFormat`. `WarnOnUnknownTag` (default true)
logs once per tag type in that context when a queried tag does not
round-trip. After 128 tag types, further unknown tag warnings in that
context are suppressed. `FormatAggregateFallbackTag`
(default false) leaves the aggregate fallback tag as `{ClrTypeName}:{id}`.
Leave that flag false on a store that already has events. See
[Tagging](../TAGGING.md).

`ContextAwareCommandDispatcher` publishes inbound `MessageContext` (and
continues `traceparent`) before `HandleAsync`. `HandleCommandAsync` sets
envelope `CausationId` to the command id when the caller left it unset.
Envelope fields: [Metadata](../METADATA.md).

`IAggregateRepository` loads by `Guid` (`{type}:{id}` / `{tenant}:{type}:{id}`)
or by `string streamId` when the stream is a custom identity. Prefer
`GetOrCreateAsync<T>(streamId)` over setting `StreamId` yourself.

`WithCommandHandlers` scans handler assemblies and registers
`ICommandDispatcher` (Core). `UseInMemory` registers the same dispatcher.

Snapshot write durability (synchronous capture, wait-free enqueue, drop-oldest,
health) and replace-in-place retention: [SNAPSHOTS.md](../SNAPSHOTS.md). Custom stores use
`EventSourcingRepositories` and `AddSnapshotWriteInfrastructure` so snapshot
writes still enqueue.

## New events signal

Each in-memory store keeps a `NewEventsSignal` (`ISignalsNewEvents.NewEvents`).
After a save commits, the store calls `Notify` with the newest global position.
The call happens only after the commit. The signal carries no events.

A reader calls `TakeTicket` before it reads. It then waits on that ticket or
on `SubscriptionPollInterval` (default 1 second), whichever comes first, and
reads again from its own checkpoint. A save that commits after the ticket
wakes the reader at once. A missed or extra signal still delivers on the timer.

The same object is public. A listener outside the store, such as a later
Postgres notification loop, can call `Notify` on it. SQL Server uses this
signal for saves in this process. Saves from another process are still picked
up on `SqlSubscriptionPollInterval`.

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
