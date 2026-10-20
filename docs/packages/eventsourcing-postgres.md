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

Tables live in `SchemaName`. `UsePostgres` sets that to `lawndart` for the
`default` context and to the context name for every other context. The
callback can change it. The name `public` is rejected.

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
is false. The in-memory store uses this same check. A new in-memory store
requires a tenant id unless `RequireTenantId` is false.

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
0. Pass `expectedVersion: -1` to require a new stream. After that statement
is sent, the store does not send it again. A cancellation after that
statement is sent is handled the same way. If a later read cannot tell
whether the rows were saved, the call throws `AppendOutcomeUnknownException`.
Read the stream before sending the append again.

A normal append takes a shared lock per tag, plus one shared lock for the
store. A dynamic consistency boundary append takes an exclusive lock on each
tag in its condition, and the same shared locks on the tags it writes.
If any condition item has no tags, that append takes the exclusive store
lock as well, so a type check is covered even when another item names a tag.
More than 64 distinct tags on one append uses the exclusive store lock
instead of one lock per tag. Two different tags that hash to the same key
only wait. They do not write the wrong row.

The caught-up sequence stays behind an unfinished append. `GetCurrentSequenceAsync`
does not move past that hole while the writer is still open.
`ReadHorizonGracePeriod` (default 30 seconds) is how long readers wait. After
that they move to the highest committed sequence and log a warning. The
warning names the session and how long the write has been open. A row that
commits below a head that already moved past it is logged as an error.
`Timeout.InfiniteTimeSpan` waits until the write ends.

## Subscriptions

`Subscribe` returns one continuous read. Catch-up and later events are the
same stream. The caller keeps the resume position.

Each read asks for one page (`SubscriptionBatchSize`, default 500) and stops
at the caught-up sequence. The scan cursor then moves to the highest position
examined, including a position that did not match the filter. A read that
loses the connection is tried again. The store logs one warning, waits, and
tries again, with a longer wait each time, up to a limit. When the read works
again it logs that the connection is back. A wrong host, a name that does not
resolve, or a TLS error ends the subscription with
`EventSubscriptionFailedException`. Cancel and dispose end it without an error.

An append sends a Postgres NOTIFY wake-up in the same transaction when
`NotifyOnAppend` is true (the default). NOTIFY is Postgres's wake-up message.
The channel name starts with `ld_`. A long schema is shortened and a short
hash is added so the name fits in 63 bytes and two schemas stay apart.
NOTIFY and LISTEN use that same name. The message is the newest position.
It carries no event. The events table is the record. A lost wake-up delays
the next read.

One listener per store reads that channel. It uses `ListenConnectionString`
when that is set, and `ConnectionString` otherwise. The listener does not
keep a transaction open. It reads a burst, then waits. If the connection
drops, it waits with a growing pause, connects again, and subscribers read
from their checkpoints.

`BackupPollInterval` (default 1 second) is how long a subscription waits
between reads while the listener is connected. A wake-up ends the wait
early. `PollOnlyInterval` (default 100 milliseconds) is the wait when the
listener is not used. A save in this process also ends the wait early, on
either interval.

At startup the listener opens its connection and sends a probe wake-up.
If the connection cannot be opened, subscriptions poll and the listener
keeps trying. If the connection opens but the probe does not come back, or
the host name contains `pooler` or `pgbouncer`, or the port is 6543, the
store logs one warning and stops trying to listen. Set
`ListenConnectionString` to a direct Postgres connection, or to a session
pooler, when the app connection goes through a transaction pooler. PgBouncer
in transaction mode cannot keep a listen session. On that app connection,
set Npgsql's `No Reset On Close=true`.

Set `NotifyOnAppend` to false to skip the wake-up. Subscriptions then use
`PollOnlyInterval`. A save in this process still wakes them.

`PostgresNotificationQueueHealthCheck` reads `pg_notification_queue_usage()`,
the fraction of the notification queue in use. The result is healthy below
0.5, degraded from 0.5, and unhealthy from 0.8. Pass other marks to the
constructor.

```csharp
services.AddHealthChecks()
    .AddCheck(
        "lawndart-postgres-notifications",
        new PostgresNotificationQueueHealthCheck(connectionString));
```

An append that sends a wake-up fails when that queue is full. A listener
that has stopped reading is the usual cause.

## Later in the 0.7 line

The outbox, inbox, snapshot tables, and projection views ship after this
event store. `EnableOutbox = true` throws when the store is built. Leave it
false until that release.
