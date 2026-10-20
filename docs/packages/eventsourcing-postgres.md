# LawnDart.EventSourcing.Postgres

Postgres implementation of `IEventLog` plus the typed `IEventStore` session.
Same repositories and domain types as InMemory. The log stores records
(event name, schema version, payload-format number, payload, metadata JSON).
The session serializes on append and reads them back into objects.

Postgres 15 or newer. Npgsql 10.

## Registration

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .UsePostgres(o =>
    {
        o.ConnectionString = connectionString;
        o.RequireTenantId = false;
    });
```

`UsePostgres` copies `LawnDartOptions.ValidateNames` onto
`PostgresEventStoreOptions.ValidateNames` before your callback runs. The
callback can change it.

Keyed `IEventStore`, `IEventLog`, `IStreamRegistry`,
`IEventStoreSubscriptions`, `IAggregateRepository`, and `IDcbRepository`
use the bounded context name. The context named `default` also gets unkeyed
aliases.

Call `InitializeSchemaAsync` on `PostgresEventStore` before the first append.
The call creates the schema if it is missing. A second call with the same
bucket count succeeds. A different bucket count is rejected. Drop and
recreate the schema to change it. An unknown format number in `schema_info`
is rejected the same way.

## Schema

Tables live in `SchemaName`. The default is `lawndart`. A context other than
the default should use its own schema name. The name `public` is rejected.

Events, streams, and tags are partitioned two ways. The top level is a list
on `tenant_id` with one default partition. That partition is hashed into
`PartitionCount` buckets. The default is 8. The allowed range is 1 to 64,
chosen when the schema is created.

A stream id with three or more colon-separated parts stores the first part
as the tenant key. Any other stream id stores the tenant key `default`.
The public stream metadata still reads the tenant from the stream id, so a
stream without a tenant prefix does not report `default`.

`RequireTenantId` true (the default) rejects a missing tenant before that
key is chosen. An empty tenant id is rejected. The table also rejects an
empty tenant id. `default` in any casing is reserved unless `ValidateNames`
is false.

Stream ids and tags use the `C` collation. `acme:Order:1` and `acme:order:1`
are different streams. Tags compare the same way.

Event ids are unique per tenant. The same event id on two tenants is two
rows. A repeated event id in one tenant throws `EventAlreadyAppendedException`.

One sequence, `event_position`, numbers every event in the schema. A losing
version check does not take a number.

## Payloads

Payload-format number 1 (JSON) is stored as jsonb. Spacing and key order can
change. The decoded event is the same. A JSON payload that contains a null
character, or that jsonb cannot store, is stored as bytes and comes back
unchanged. Any other payload-format number is stored as bytes.

Metadata is jsonb. Snapshot bytes and outbox bytes are not in this release.

## Append

One function call writes the stream row, the events, and the tags. The
version check runs before any sequence number is taken. A new stream's first
event is version 1. A dynamic consistency boundary stream starts at version
0. Pass `expectedVersion: -1` to require a new stream.

A normal append takes a shared lock per tag, plus one shared lock for the
store. A dynamic consistency boundary append takes an exclusive lock on each
tag in its condition. A condition with no tags takes the store lock
exclusively, so other appends wait. More than 64 distinct tags on one append
uses the store lock instead of one lock per tag. Two different tags that
hash to the same key only wait. They do not write the wrong row.

The caught-up sequence stays behind an unfinished append. `GetCurrentSequenceAsync`
does not move past that hole while the writer is still open.
`ReadHorizonGracePeriod` (default 30 seconds) is how long readers wait. After
that they move to the highest committed sequence and log a warning. The
warning names the session and how long the write has been open. A row that
commits below a head that already moved past it is logged as an error.
`Timeout.InfiniteTimeSpan` waits until the write ends.

## Later in the 0.7 line

The outbox, inbox, snapshot tables, and projection views ship after this
event store. `EnableOutbox = true` throws when the store is built. Leave it
false until that release.

An append sends a notification whose payload is the newest position. A
same-process reader wakes from the in-process signal after the save commits.
The listener that reads the notification from another process ships with
live subscriptions. Until then, the live subscription suite is skipped on
Postgres with a reason.
