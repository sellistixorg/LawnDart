# Event schema versioning

The catalog token is a **family** name. It stays stable when the payload
shape changes. `SchemaVersion` on the log frame selects which CLR type to
deserialize. Typed reads then upcast that stored type to this process's
current type. `GetName` returns the family token, not `author-registered.v2`.

## Log and session

`IEventLog` is the durable log: append `AppendEvent`, read `RecordedEvent`.
Each frame carries the family token, first-class `SchemaVersion`,
`CodecId`, payload bytes, UTF-8 JSON metadata bytes, tags, stream
id/version, global sequence, and commit timestamp. Stores persist frames.
They do not resolve CLR types.

`IEventStore` is the typed session over that log. Handlers, aggregates,
DCB, Lightweight replay, time-travel, and the outbox publisher stay on
`IEvent` / `SequencedEvent`. The session serializes on the way in,
resolves `(token, SchemaVersion)` on the way out, deserializes the stored
version's CLR type, then upcasts to current.

Upcast runs in your host. Register hops with `WithUpcasters`. Typed append
writes only this process's current type. There is no downcast API.

## Implement a store

A third-party backend implements `IEventLog`. Application code keeps
`IEventStore`.

- Append `AppendEvent`: family token, `SchemaVersion`, `CodecId`,
  payload bytes, UTF-8 JSON metadata bytes, tags. Snapshot payload and
  metadata. Copy the bytes.
- Return `RecordedEvent` with store-assigned stream id, stream version,
  global sequence, and commit timestamp. Those fields come from the store,
  not from metadata JSON.
- Query, concurrency, and filters stay on tokens, tags, and sequence.
- Push, if you offer it, is `IEventLogSubscriptions` (recorded events).
  The shipped adapter hydrates `IEventStoreSubscriptions`.
- `ConsistencyMarker` on append/query results is store-owned and passes
  through the adapter unchanged.

Signatures: [core package](packages/core.md#portable-store-contract).

## Family token

Every concrete `IEvent` that is written needs `[EventTypeName]`. Prefer
kebab-case (`author-registered`). The token stays the family name across
versions. CLR `FullName` is not stored. Register the family with
`WithEventTypes`. An unknown token (including an old FullName /
AssemblyQualifiedName string) fails closed.

`WithEventTypes` calls `EventTypeCatalog.Materialize` and registers that
**scoped immutable** catalog on the bounded context. A host that skips it
fails at startup. Two contexts do not share a catalog; the same token may
map to different CLR types in each.

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

`WithEventTypes` / `EventTypeCatalog.Materialize` fails if a family has two
currents, or two or more types and no `current: true`. A single one-arg
attribute is not an error.

## This process writes only current

Typed append (`IEventStore` / `EventSession`) rejects a historical CLR type
for a family the catalog already knows. The session stamps frame
`SchemaVersion` from the current type and mirrors it onto
`EventMetadata.SchemaVersion` (a caller-supplied integer is overwritten on
the snapshot copy). See [Metadata](METADATA.md).

On read, the **frame** integer is authority, not the metadata blob. A
constructed frame with `0` becomes `1`. Drop and recreate older SQL event
and outbox tables: `SchemaVersion` and `CodecId` have no column defaults,
and an older table throws and names drop-and-recreate. Old binaries may
still append the version that was current for them; the log accepts those
frames.

## Default codec

The shipped session codec is System.Text.Json (`IEventSerializer` as
`ReadOnlyMemory<byte>` UTF-8, plugin identity `application/json`). One
codec per session. The durable frame stores `CodecId`, not a MIME string.
A stored codec id that does not match the session fails closed.

MemoryPack, protobuf, and Avro are optional session codecs. Keep
`[PropertyOrder]` on events so those positional formats share one model.
STJ ignores the numbers. The **field number** is the contract, not source
order. Reusing a number or changing the meaning at that number is a
layout break: bump `SchemaVersion`. Reordering properties in the file
with stable numbers is additive.

## Writing an upcaster

Register hops with `WithUpcasters` after `WithEventTypes`. One class, one
hop, forward only. A v1 to v3 family is either a direct upcaster or a
chain (`v1` to `v2`, `v2` to `v3`).

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

`EventUpcastPipeline.Materialize` / `WithUpcasters` fails if any historical
version in the catalog cannot reach current. That warmup is the runtime
authority. `LawnDart.Analyzers` reports the same rules at `dotnet build`
(`LDT001` two currents, `LDT002` multi-type family with no `current: true`,
`LDT003` incomplete upcaster chain). One-arg `[EventTypeName("token")]` is
not a diagnostic. A green analyzer still needs warmup.

Typed hydrate (`IEventStore` / `EventSession` / outbox publish) deserializes
the stored version, then applies that chain. The value on
`EventMetadata.SchemaVersion` stays the frame integer. Aggregates, DCB,
Lightweight replay, and time-travel all see the current CLR type because
they read through the session. A missing hop throws
`MissingEventUpcasterException`.

## Deploy

Additive JSON rolls freely: new named properties on the current type do not
require a `SchemaVersion` bump. A breaking change (renamed meaning, a
removed required field, or a positional-codec layout change: a reused or
remapped `[PropertyOrder]` number) is expand-contract. Ship readers that
understand `SchemaVersion` N+1 before any process writes N+1. Old binaries
fail closed on those newer rows (`EventSchemaTooNewException`) and may keep
appending the version that was current for them.

## When typed read fails

These apply to the typed session only. `IEventLog` and raw copy return the
frame.

| Condition | Exception |
|---|---|
| Stored `SchemaVersion` newer than this process's current for a known family | `EventSchemaTooNewException` |
| Unknown family token | `UnknownEventFamilyException` |
| Known family, historical CLR type not in this catalog | `EventSchemaNotInCatalogException` |
| Stored codec id ≠ session codec | `EventContentTypeMismatchException` |
| Payload will not deserialize as the stored type | `EventPayloadException` |
| Historical type present, no upcaster to current | `MissingEventUpcasterException` |

Deploy the missing type or upcaster. An upcaster that throws fails closed
the same way: the session does not skip the frame. A typed subscription
does not advance its cursor past the failed frame. The Lightweight runner
logs the stuck sequence and waits five seconds; it does not hot-loop.

See [Glossary](GLOSSARY.md) (catalog token, SchemaVersion, event log,
event store). [Eventhesis](EVENTHESIS.md) keeps the token when you
hand-write events.
