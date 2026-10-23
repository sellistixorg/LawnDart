# Postgres partitions and tenants

Events, streams, and tags are partitioned by tenant. The layout is chosen
when the schema is created. Snapshots, the outbox, the inbox, projection
views, checkpoints, and the runner lease table are not partitioned.
Snapshot rows still store `tenant_id`.

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
- `RequireTenantId` true (the default) rejects a missing tenant before the
  key is chosen.
- With `RequireTenantId` false and no tenant prefix, the row stores
  `default`. Stream metadata still reads the tenant from the stream id, so
  that stream does not report `default`.

`acme:Order:1` stores tenant `acme`. `Order:1` stores tenant `default`.
`:order:1` has an empty first part, so it stores `default` rather than an
empty string.

A stream id prefix with three or more colon-separated parts, and a
non-empty first part, limits a list to that tenant. A shorter prefix can
match more than one tenant.

## Buckets

Two levels:

1. The top level is a list on `tenant_id`, with one default partition.
2. That default partition is hashed on `tenant_id` into `PartitionCount`
   buckets.

`PartitionCount` defaults to 8. The allowed range is 1 to 64. Set it before
the first `InitializeSchemaAsync`. A later open with a different count, or
with partitioning turned off, is rejected. Drop and recreate the schema to
change the layout.

A unique index on a partitioned table includes `tenant_id`. The primary key
on events is `(tenant_id, stream_id, version)`. Event ids are unique per
tenant: `(event_id, tenant_id)`. Global position is unique per tenant:
`(global_position, tenant_id)`. One sequence, `event_position`, numbers
every event in the schema. A losing version check does not take a number.

With the outbox on, the outbox row id is the event id, so that id is unique
in the whole schema. See [Backend differences](BACKEND_DIFFERENCES.md).

## Related

- [Postgres hosting](POSTGRES_HOSTING.md)
- [Postgres event store](packages/eventsourcing-postgres.md)
- [Schema initialization](SCHEMA_INITIALIZATION.md)
- [Stream IDs](STREAM_IDS.md)
