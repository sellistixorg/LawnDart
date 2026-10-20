# Event schema versioning

The catalog token is the **event name**. It stays stable when the payload
shape changes. `SchemaVersion` on the stored record selects which .NET type
to deserialize. Typed reads then upcast that stored type to this process's
current type. `GetName` returns the event name, not `author-registered.v2`.

## Log and session

`IEventLog` is the log that keeps records: append `AppendEvent`, read
`RecordedEvent`. Each record carries the event name, `SchemaVersion`, the
payload-format number `CodecId`, payload bytes, UTF-8 JSON metadata bytes,
tags, stream id/version, global sequence, and commit timestamp. Stores
persist records. They do not resolve .NET types.

`IEventStore` is the typed session over that log. Handlers, aggregates,
dynamic consistency boundary (DCB), Lightweight replay, time-travel, and
the outbox publisher stay on `IEvent` / `SequencedEvent`. The session
serializes on the way in, resolves `(token, SchemaVersion)` on the way out,
deserializes the stored version's .NET type, then upcasts to current.

Upcast runs in your host. Register hops with `WithUpcasters`. Typed append
writes only this process's current type. There is no downcast API.

## Implement a store

A third-party backend implements `IEventLog`. Application code keeps
`IEventStore`.

- Append `AppendEvent`: event name, `SchemaVersion`, payload-format number
  `CodecId`, payload bytes, UTF-8 JSON metadata bytes, tags. Snapshot
  payload and metadata. Copy the bytes.
- Return `RecordedEvent` with store-assigned stream id, stream version,
  global sequence, and commit timestamp. Those fields come from the store,
  not from metadata JSON.
- Query, concurrency, and filters stay on event names, tags, and sequence.
- Push, if you offer it, is `IEventLogSubscriptions` (recorded events).
  The shipped adapter reads those records into objects for
  `IEventStoreSubscriptions`.
- `ConsistencyMarker` on append/query results is store-owned and passes
  through the adapter unchanged.

Signatures: [core package](packages/core.md#portable-store-contract).

## Event name

Every concrete `IEvent` that is written needs `[EventTypeName]`. Prefer
kebab-case (`author-registered`). The token stays the event name across
versions. The .NET type's `FullName` is not stored. Register the event name
with `WithEventTypes`. An event name this process has not registered,
including an old `FullName` or `AssemblyQualifiedName` string, makes the
typed read throw.

`WithEventTypes` calls `EventTypeCatalog.Materialize` and registers that
catalog on the bounded context. The catalog does not change. A host that
skips it throws at startup. Two contexts do not share a catalog. The same
event name may map to different .NET types in each.

## One type

```csharp
[EventTypeName("author-registered")]
public record AuthorRegistered(Guid Id, DateTime Timestamp, string Name) : IEvent;
```

One-arg `[EventTypeName("token")]` is version 1 and implicitly current.

## Two or more types

The **current** type keeps the domain name. Historical types take a `V1` /
`V2` suffix. Mark exactly one `current: true`. Current is not inferred
from the highest integer.

```csharp
[EventTypeName("author-registered", version: 1)]
public record AuthorRegisteredV1(Guid Id, DateTime Timestamp, string Name) : IEvent;

[EventTypeName("author-registered", version: 2, current: true)]
public record AuthorRegistered(Guid Id, DateTime Timestamp, string Name, string Bio) : IEvent;
```

`WithEventTypes` / `EventTypeCatalog.Materialize` throws if an event name
has two currents, or two or more types and no `current: true`. A single
one-arg attribute is not an error.

## This process writes only current

Typed append (`IEventStore` / `EventSession`) rejects a historical .NET type
for an event name the catalog already knows. The session stamps
`SchemaVersion` on the stored record from the current type and mirrors it
onto `EventMetadata.SchemaVersion` (a caller-supplied integer is overwritten
on the snapshot copy). See [Metadata](METADATA.md).

On read, the integer on the **stored record** is authority, not the metadata
blob. A constructed record with `0` becomes `1`. Drop and recreate older SQL
event and outbox tables: `SchemaVersion` and the payload-format column
`CodecId` have no column defaults, and an older table throws and names
drop-and-recreate. Old binaries may still append the version that was
current for them. The log accepts those records.

## Default payload format

The shipped session format is System.Text.Json (`IEventSerializer` as
`ReadOnlyMemory<byte>` UTF-8, plugin identity `application/json`). One
payload format per session. The stored record keeps a payload-format number
(`CodecId`). The media type stays on `IEventSerializer.ContentType`.
If that number is not the format this session uses, the typed read throws
and does not decode the payload.

MemoryPack, protobuf, and Avro are optional session formats. Keep
`[PropertyOrder]` on events so those positional formats share one model.
System.Text.Json ignores the numbers. The **field number** is the contract,
not source order. Reusing a number or changing the meaning at that number
is a layout break: bump `SchemaVersion`. Reordering properties in the file
with stable numbers is additive.

## Writing an upcaster

Register hops with `WithUpcasters` after `WithEventTypes`. One class, one
hop, forward only. An event name that moves from version 1 to version 3
uses either a direct upcaster or a chain (version 1 to version 2, version 2
to version 3).

```csharp
public sealed class AuthorRegisteredV1ToV2 : IEventUpcaster<AuthorRegistered, AuthorRegisteredV1>
{
    public AuthorRegistered Upcast(AuthorRegisteredV1 source)
        => new(source.Id, source.Timestamp, source.Name, Bio: "");
}

var ctx = services.AddBoundedContext("default")
    .WithEventTypes(typeof(AuthorRegisteredV1), typeof(AuthorRegistered))
    .WithUpcasters(typeof(AuthorRegisteredV1ToV2));
```

`EventUpcastPipeline.Materialize` / `WithUpcasters` throws if any historical
version in the catalog cannot reach current. That warmup is the runtime
authority. `LawnDart.Analyzers` reports the same rules at `dotnet build`
(`LDT001` two currents, `LDT002` an event name with more than one type and
no `current: true`, `LDT003` incomplete upcaster chain). One-arg
`[EventTypeName("token")]` is not a diagnostic. A green analyzer still
needs warmup.

A typed read (`IEventStore` / `EventSession` / outbox publish) deserializes
the stored version, then applies that chain. The value on
`EventMetadata.SchemaVersion` stays the integer on the stored record.
Aggregates, DCB, Lightweight replay, and time-travel all see the current
.NET type because they read through the session. A missing hop throws
`MissingEventUpcasterException`.

## Deploy

Additive JSON rolls freely: new named properties on the current type do
not require a `SchemaVersion` bump. A breaking change (renamed meaning, a
removed required field, or a positional-format layout change: a reused or
remapped `[PropertyOrder]` number) ships in two steps. Ship readers that
understand `SchemaVersion` N+1 before any process writes N+1. Old binaries
throw `EventSchemaTooNewException` on those newer rows and leave them
unread. They may keep appending the version that was current for them.

## When typed read fails

These apply to the typed session only. `IEventLog` and a raw copy return
the stored record.

| Condition | Exception |
|---|---|
| Stored `SchemaVersion` newer than this process's current for a known event name | `EventSchemaTooNewException` |
| Event name not in this process's catalog | `UnknownEventFamilyException` |
| Known event name, historical .NET type not in this catalog | `EventSchemaNotInCatalogException` |
| Stored payload format is not this session's format | `EventContentTypeMismatchException` |
| Payload will not deserialize as the stored type | `EventPayloadException` |
| Historical type present, no upcaster to current | `MissingEventUpcasterException` |

Deploy the missing type or upcaster. An upcaster that throws stops the read
the same way: the session does not skip the record. A typed subscription
does not advance its cursor past the failed record. The Lightweight runner
logs the stuck sequence and waits five seconds before it reads again.

See [Glossary](GLOSSARY.md) (catalog token, SchemaVersion, event log,
event store).
