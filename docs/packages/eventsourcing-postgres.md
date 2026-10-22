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

`ListStreamsAsync` returns active streams ordered by stream id. The page
uses `(tenant_id, stream_id)`. A prefix with three or more colon-separated
parts, and a non-empty first part, limits the read to that tenant. A shorter
prefix can match more than one tenant. Prefix match is exact, including
case. Stream lists and stream counts include rows whose status is `Active`.

The aggregate type index is `(aggregate_type, stream_id)`. Stream tags for
the page live in `stream_tags` (`tenant_id`, `tag`, `stream_id`). The
primary key is those three columns, and the table uses the same tenant hash
buckets as `streams`. A tag page reads `stream_tags` from the last stream id,
then loads that stream row. A trigger on `streams` updates `stream_tags`
when a row is inserted, when its tags change, and when the row is deleted.
Replacing the append function does not remove the trigger, so a save from an
older build still updates the tag rows. `InitializeSchemaAsync` creates the
table when it is missing. It copies existing tags in batches after the schema
lock is released, and records that copy so a later start does not copy them
again. The format stamp stays at 1.

Event ids are unique per tenant. The same event id on two tenants is two
rows. A repeated event id in one tenant throws `EventAlreadyAppendedException`.
With the outbox on, the same event id on two tenants also throws that
exception. See [Outbox](#outbox).

One sequence, `event_position`, numbers every event in the schema. A losing
version check does not take a number.

## Payloads

Payload-format number 1 (JSON) is stored as jsonb. Spacing and key order can
change. The decoded event is the same. A JSON payload that contains a null
character, or that jsonb cannot store, is stored as bytes and comes back
unchanged. Any other payload-format number is stored as bytes.

Metadata is jsonb. Outbox payload is the original bytes. Snapshot state
is bytes. See [Snapshots](#snapshots) and [Outbox](#outbox).

## Append

One function call writes the stream row, the events, and the tags. The
version check runs before any sequence number is taken. A new stream's first
event is version 1. A dynamic consistency boundary stream starts at version
0. Pass `expectedVersion: -1` to require a new stream. After a result row from
that statement has been read, the store does not send it again. A
cancellation after that point is handled the same way. If a later read
cannot tell whether the rows were saved, the call throws
`AppendOutcomeUnknownException`. Read the stream before sending the append
again.

A closed connection is judged by what the store can see. When it can tell
the append was not saved, it drops that connection and tries the append
once. It logs one warning. The try uses the same cancellation token. When
it cannot tell, it reads the event ids on that stream. If every event
is stored there under the same event name, the call returns them. If the
read still cannot tell, the call throws
`AppendOutcomeUnknownException`. Read the stream before sending the append
again. A version conflict, a condition failure, a constraint failure, and a
rejected login are reported as those errors when the events are not stored.

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
is refused or that times out is tried again, including the first read. A
temporary failure to look up the host name is tried again the same way. The
store logs one warning, waits, and tries again, with a longer wait each time.
When the read works again it logs that the connection is back. Once those
tries run out, the subscription ends with an error. A wrong host, a name
that does not resolve, or a TLS error ends the subscription with
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
either interval. The wait is chosen again each time. An outage uses the
poll-only interval.

At startup the listener opens its connection and sends a probe wake-up.
If that connection cannot be opened, the listener keeps trying for as long
as a subscription is still reading. When the subscription ends, the listener
stops. If the connection opens but the probe does not come back, or the
host name contains `pooler` or `pgbouncer`, or the port is 6543, the store
logs one warning and stops trying to listen. Set
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

## Snapshots

`UsePostgres(...).WithSnapshots(...)` registers the snapshot stores. Call
`InitializeSchemaAsync` so `event_snapshots` and `dcb_snapshots` exist.
One row per stream, and one row per dynamic consistency boundary hash. A
write replaces that row. State is `bytea`. The checksum is a SHA-256 of
those bytes. A missing snapshot, a bad checksum, or a snapshot that cannot
be read returns empty and the repository replays the events.

A dynamic consistency boundary snapshot is stored under the same 64-character
hash as on SQL Server. The plaintext load tags are stored beside the hash.
After a restore, the repository replays events that landed after the
snapshot.

## Outbox

`EnableOutbox` writes one outbox row for each appended event in the same
append statement as the events. A rolled-back append leaves no outbox row.
The row id is the event id. The payload is the bytes that were appended.
Metadata is jsonb, the same value stored on the event.

With the outbox on, that id is unique in the whole schema. The same event id
on two tenants throws `EventAlreadyAppendedException`. With the outbox off,
event ids are unique per tenant.

`GetUnprocessedAsync` claims rows in position order. It sets `claimed_until`
to the current time plus `OutboxClaimLease` (default 30 seconds) and skips a
row another claim already holds. A claim that has expired can be claimed
again. `TimeSpan.Zero` lets the next poll claim the row again. Marking a row
processed, or recording a failure, clears the claim. A dead-lettered row stays
out of the claim until it is reset. Reset clears the dead-letter time and sets
attempts to 0.

The table is `outbox` in the context schema. It is not partitioned. An index
on `sequence_position` covers rows that are not processed and not dead-lettered.
When the table is created, autovacuum uses a vacuum scale factor of 0.01, a
vacuum threshold of 1000, an analyze scale factor of 0.02, and an analyze
threshold of 1000.

`InitializeSchemaAsync` adds the table when it is missing. The events format
stamp stays at 1. A process still running 0.7.0-alpha.1 calls the original
append function and can keep appending. That process does not write outbox
rows, and the store does not report that.

Deploy this version with `EnableOutbox` false until no 0.7.0-alpha.1 copy can
run. If your host keeps older versions of the app running, stop them first.
Then set `EnableOutbox` to true.
On a brand-new database during a mixed-version deploy, initialize the event
store with the new version first, because the 0.7.0-alpha.1 event store cannot
initialize a schema that the new inbox created first.

```csharp
services.AddBoundedContext("default")
    .UsePostgres(o =>
    {
        o.ConnectionString = connectionString;
        o.EnableOutbox = true;
    });
```

Register an `IOutboxPublisher` before the host starts. `UsePostgres` starts
the outbox processor for that context when `EnableOutbox` is true.

The inbox is `LawnDart.Messaging.Postgres`. See
[Messaging.Postgres](messaging-postgres.md). Projection views are in
`LawnDart.Projections.Lightweight.Postgres`.
