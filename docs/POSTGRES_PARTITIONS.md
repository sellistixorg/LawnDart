# Postgres partitions and tenants

The events table and the event tags table are split into ranges of global
position. The layout is chosen when the schema is created. Streams, stream
tags, snapshots, the outbox, the inbox, projection views, checkpoints, and
the runner lease table are plain tables. Every row still stores `tenant_id`.

These tables run on Postgres 15 or newer. The `C` collation compares raw bytes, so stream ids
and tags match exactly, including case. See
[Backend differences](BACKEND_DIFFERENCES.md).

## Tenant key

One rule on every write and every read:

- A stream id with three or more colon-separated parts stores the first
  part as the tenant key, when that part is not empty.
- Any other stream id stores the tenant key `default`.
- An empty tenant id is rejected. The table check rejects an empty
  `tenant_id` as well.
- `default` in any casing is a reserved tenant id when `ValidateNames` is
  true (the default). Set `ValidateNames` to false to keep an existing
  tenant id of `default`.
- `Tenancy.Shared` (the default) rejects a missing tenant before the
  key is chosen.
- With `Tenancy.Single` and `SingleTenantId` null, a stream without a tenant
  prefix stores `default`. Stream metadata still reads the tenant from the
  stream id, so that stream does not report `default`. With `SingleTenantId`
  set, streams use `{SingleTenantId}:{type}:{id}` and store that tenant id.

`acme:Order:1` stores tenant `acme`. `Order:1` stores tenant `default`.
`:order:1` has an empty first part, so it stores `default` rather than an
empty string.

A stream id prefix with three or more colon-separated parts, and a
non-empty first part, limits a list to that tenant. A shorter prefix can
match more than one tenant.

## Position ranges

`events` and `event_tags` are partitioned by range on `global_position`.
Each range is its own table, named `events_p{n}` and `event_tags_p{n}`.
Range `n` holds positions `n * PartitionSize + 1` to `(n + 1) * PartitionSize`.

These ranges are only a storage layout. They are not the projection work split
by partition hash (`PartitionFilter`, `QueryItem.ForPartition`,
`ConsistentHashPartitioningService`), which works the same with ranges on or off.

- `PartitionSize` defaults to 100,000,000 positions. It must be from 10,000
  to 10^15.
- `PositionPartitioning` turns ranges on or off. When it is null (the
  default), ranges are on for `Tenancy.Single` and `Tenancy.Shared`.
  `Tenancy.Isolated` is refused at startup in this release. With ranges off,
  both tables are plain tables.
- The choice and the size are stamped in `schema_info` (`partitioning` and
  `partition_size`) on the first `InitializeSchemaAsync`. A later open with
  ranges turned the other way, or with a different size, is rejected. Drop
  and recreate the schema to change the layout.
- With ranges on, the events and tags table names must be 40 bytes or
  shorter, so the range names fit in a Postgres name.

### Ranges made ahead of time

The store keeps two empty ranges above the range that holds the newest
position:

- `InitializeSchemaAsync` creates them, on the setup connection.
- After an append, when the newest position gets within two ranges of the
  last range the store knows of, the store adds the next ranges in the
  background. The append does not wait for this. When that background work
  fails, the store logs one warning and waits 30 seconds before it tries
  again. It also waits 30 seconds when it makes no progress, because the
  newest position the store saw is above what the sequence handed out, for
  example after a restore. `EnsurePartitionsAsync` makes ranges only up to
  the sequence.
- An append that reaches past the last range the store knows of checks for
  itself, inside its own transaction, and adds the missing range first. When
  another append attached that range a moment earlier, the insert can still
  use the list of ranges its transaction read before; the store then tries
  the append again in a new transaction, as it does after a deadlock. With
  the grants below, an append does not fail because a range is missing.
- `EnsurePartitionsAsync` does the same on demand, for an admin tool or
  after a bulk load that moved the position sequence.

All of this goes through one function, `ensure_event_partitions`. It runs as
the schema owner, with a fixed search path, so the app role does not need
rights to create tables. It takes an advisory lock while it adds ranges, so
many nodes can call it at once and each range is created once. It also works
inside a repeatable read or serializable transaction: a range another caller
made in the meantime counts as made. It creates each new range as a separate
table and then attaches it, which does not block appends or reads on the
other ranges. It never recreates a range below the newest one, so a range
that was detached to archive old events stays gone. It only makes ranges up
to a few above the newest position handed out.

### Grants for the app role

When `InitializeSchemaAsync` runs with `MigrationConnectionString`, it grants
the app role what it needs. A role set up by hand needs:

```sql
GRANT USAGE ON SCHEMA lawndart TO app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lawndart TO app;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA lawndart TO app;
GRANT EXECUTE ON FUNCTION lawndart.append_to_stream TO app;
```

- `ensure_event_partitions` is granted to `PUBLIC` when it is created. Calling
  it still needs `USAGE` on the schema, which the app role has. If your
  database removes `PUBLIC` rights on functions, also grant `EXECUTE` on
  `ensure_event_partitions` to the app role. Without it, an append past the
  ranges made so far fails.
- Range tables made later need no grants. Reads and writes go through the
  parent tables.

### Keys and indexes

- The primary key on `events` is `global_position`. One sequence,
  `event_position`, numbers every event in the schema. A losing version
  check does not take a number.
- Stream reads use a non-unique index on `(tenant_id, stream_id, version)`.
  Each range has its own copy of that index. A stream read cannot narrow by
  position, so it looks in every range, including the empty ones ahead. The
  cost of a stream read grows with the number of ranges. The `streams` row
  holds the stream version and is what rejects a second write of the same
  version.
- Event type reads use `(event_type_id, global_position)`.
- With `Tenancy.Shared`, reads for one tenant use `(tenant_id, global_position)`.
- A small block range index on `recorded_at` serves time limits.
- The primary key on `event_tags` is `(tag, global_position, tenant_id)` and
  also carries the event type, so a tag and type read does not need to visit
  `events`.
- Tag tables are created with `parallel_workers = 0`. A tag read returns one
  page of results, and a parallel plan costs more to start than the read
  itself.

### Event ids and retried appends

Event ids are not unique on Postgres, and there is no index on them. Two
appends that use the same event id are both saved.

The expected version on a stream append and the condition on a dynamic
consistency boundary append are what stop a repeat. An unconditional append
retried after a lost reply can be saved twice. Use an expected version or a
condition to make retries safe: the second try then fails that check and
writes nothing.

When the connection closes before the store reads a reply, the store reads
the append's event ids back before it tries again, and returns the saved rows
when it finds them. A first try that commits after that read is not seen, so
an unconditional append can still be saved twice. An append that fails because
a range was attached a moment earlier did not commit, and the store tries it
again in a new transaction.

With the outbox on, the outbox row id is the source event id, so that id is
still unique in the whole schema for published events. See
[Backend differences](BACKEND_DIFFERENCES.md).

### Many ranges and locks

A read that cannot narrow by position, such as a stream read, opens every
range and locks each range table and its indexes until the transaction ends:
about seven locks for each events range and two for each tag range. Postgres
keeps these in one shared lock table sized by `max_locks_per_transaction`
(default 64) times the number of connections. When a schema has more than a
few dozen ranges and many connections read at once, raise
`max_locks_per_transaction` to about ten times the number of ranges, use a
larger `PartitionSize`, or archive old ranges.

### Archiving old ranges

Detach an old range with `DETACH PARTITION ... CONCURRENTLY`, which does not
block appends and reads on the other ranges. Run it outside a transaction,
once for each table:

```sql
ALTER TABLE lawndart.events DETACH PARTITION lawndart.events_p0 CONCURRENTLY;
ALTER TABLE lawndart.event_tags DETACH PARTITION lawndart.event_tags_p0 CONCURRENTLY;
```

Then keep or drop the detached tables. Reads no longer see those events.
Do not detach the range that holds the newest position or the ranges ahead
of it. A detached range is not created again.

## Related

- [Postgres hosting](POSTGRES_HOSTING.md)
- [Postgres event store](packages/eventsourcing-postgres.md)
- [Schema initialization](SCHEMA_INITIALIZATION.md)
- [Stream IDs](STREAM_IDS.md)
