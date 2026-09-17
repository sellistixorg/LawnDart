# Event schema versioning

The catalog token is a **family** name. It stays stable when the payload
shape changes. `SchemaVersion` on the log frame selects which CLR type to
deserialize. Typed reads then upcast that stored type to this process's
current type. `GetName` returns the family token, not `author-registered.v2`.

## Log and session

`IEventLog` is the durable log: append `AppendEvent`, read `RecordedEvent`.
Each frame carries the family token, first-class `SchemaVersion`,
`ContentType`, payload bytes, UTF-8 JSON metadata bytes, tags, stream
id/version, global sequence, and commit timestamp. The log does not
resolve CLR types.

`IEventStore` is the typed session over that log. Handlers, aggregates,
DCB, Lightweight replay, time-travel, and the outbox publisher stay on
`IEvent` / `SequencedEvent`. The session serializes on the way in,
resolves `(token, SchemaVersion)` on the way out, deserializes the stored
version's CLR type, then upcasts to current.

LawnDart is an in-process runtime. Upcast happens in your host. The log
does not store Chronicle-style generations, downcast for old readers, or
run a server-side migrator.

## Implement a store

A third-party backend implements `IEventLog`, not `IEventStore`.

- Append `AppendEvent`: family token, `SchemaVersion`, `ContentType`,
  payload bytes, UTF-8 JSON metadata bytes, tags. Snapshot payload and
  metadata; do not retain the caller's arrays.
- Return `RecordedEvent` with store-assigned stream id, stream version,
  global sequence, and commit timestamp. Do not parse metadata JSON to
  fill those fields.
- Query, concurrency, and filters stay on tokens, tags, and sequence.
- Push, if you offer it, is `IEventLogSubscriptions` (recorded events).
  The shipped adapter hydrates `IEventStoreSubscriptions`.
- `ConsistencyMarker` on append/query results is store-owned and passes
  through the adapter unchanged.

Application code keeps `IEventStore`. Signatures:
[core package](packages/core.md#portable-store-contract).

## Family token

Every concrete `IEvent` that is written needs `[EventTypeName]`. Prefer
kebab-case (`author-registered`). The token does not change across
versions. CLR `FullName` is not stored; older FullName /
AssemblyQualifiedName SQL rows still resolve as read aliases.

`WithEventTypes` calls `EventTypeCatalog.Materialize` and registers that
**scoped immutable** catalog on the bounded context. Two contexts do not
share a mutable global catalog. `EventTypeNameResolver` remains a
process-wide compatibility wrapper for hosts that skip `WithEventTypes`.

## One type

```csharp
[EventTypeName("author-registered")]
public record AuthorRegistered(Guid Id, DateTime Timestamp, string Name) : IEvent;
```

One-arg `[EventTypeName("token")]` is version 1 and implicitly current.

## Two or more types

The **current** type keeps the domain name. Historical types take a `V1` /
`V2` suffix. Mark exactly one `current: true`. Do not infer current from the
highest integer.

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
`EventMetadata.SchemaVersion` (a caller-supplied integer is overwritten).
On read, the **frame** integer is authority — not the metadata blob, not an
implied `1` when the frame has a value. Missing or zero on old rows is `1`.
Old binaries may still append the version that was current for them; the log
accepts those frames. This process does not downcast.

## Default codec

The shipped session codec is System.Text.Json (`IEventSerializer` as
`ReadOnlyMemory<byte>` UTF-8, content-type `application/json`). One codec
per session; a stored content-type that does not match fails closed.

MemoryPack, protobuf, and Avro are optional session codecs. Keep
`[PropertyOrder]` on events so those positional formats share one model.
STJ ignores the numbers. The **field number** is the contract, not source
order. Reusing a number or changing the meaning at that number is a
layout break — bump `SchemaVersion`. Reordering properties in the file
with stable numbers is not.

## Writing an upcaster

Register hops with `WithUpcasters` after `WithEventTypes`. One class, one
hop. A v1 → v3 family is either a direct upcaster or a chain
(`v1 → v2`, `v2 → v3`). There is no downcast API.

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
authority. Compile-time `LDT` diagnostics for the same rules land in a
later analyzer package; a green analyzer is not a substitute for warmup.

Typed hydrate (`IEventStore` / `EventSession` / outbox publish) deserializes
the stored version, then applies that chain. The value on
`EventMetadata.SchemaVersion` stays the frame integer. Aggregates, DCB,
Lightweight replay, and time-travel all see the current CLR type because
they read through the session. A missing hop throws
`MissingEventUpcasterException`.

## Deploy

Additive JSON rolls freely: new named properties on the current type do not
require a `SchemaVersion` bump. A breaking change — renamed meaning, a
removed required field, or a positional-codec layout change (a reused or
remapped `[PropertyOrder]` number) — is expand-contract. Ship readers that
understand `SchemaVersion` N+1 before any process writes N+1. Old binaries
fail closed on those newer rows (`EventSchemaTooNewException`) and may keep
appending the version that was current for them. There is no remote
downcaster and no skip override.

## When typed read fails

These apply to the typed session only. `IEventLog` and raw copy return the
frame.

| Condition | Exception |
|---|---|
| Stored `SchemaVersion` newer than this process's current for a known family | `EventSchemaTooNewException` |
| Unknown family token | `UnknownEventFamilyException` |
| Known family, historical CLR type not in this catalog | `EventSchemaNotInCatalogException` |
| Stored content-type ≠ session codec | `EventContentTypeMismatchException` |
| Payload will not deserialize as the stored type | `EventPayloadException` |
| Historical type present, no upcaster to current | `MissingEventUpcasterException` |

There is no runtime override to skip a fail-closed event. Deploy the
missing type or upcaster. An upcaster that throws fails closed the same
way — the session does not skip the frame. A typed subscription does not
advance its cursor past the failed frame. The Lightweight runner logs
the stuck sequence and waits five seconds; it does not hot-loop.

See [Glossary](GLOSSARY.md) (catalog token, SchemaVersion, event log,
event store). [Eventhesis](EVENTHESIS.md) keeps the token when you
hand-write events.
