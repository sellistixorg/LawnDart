# Backend differences

In-memory, SQL Server, and Postgres share one store contract. A few
behaviors differ. The shared tests record those differences as capability
flags on `ContractCapabilities`, so a test shows the difference instead of
hiding it.

Two flags change what an append means.

| Store | `EnforcesEventIdAcrossTenants` | `StreamIdsCaseInsensitive` |
|---|---|---|
| In-memory | Yes. The same event id on a second tenant throws `EventAlreadyAppendedException`. | No. `acme:Order:1` and `acme:order:1` are different streams. |
| SQL Server | Yes. The same event id on a second tenant throws `EventAlreadyAppendedException`. | Yes, on the databases the contract tests use. A default database matches without regard to case. The match follows the database collation. |
| Postgres | No, while the outbox is off. The same event id on two tenants is two rows. A repeated id in one tenant throws `EventAlreadyAppendedException`. | No. Stream ids use the `C` collation. `acme:Order:1` and `acme:order:1` are different streams. |

With the Postgres outbox on, the outbox row id is the event id, so that id
is unique in the schema. The same event id on two tenants then throws
`EventAlreadyAppendedException`.

Tags match exactly, including case, on every store.

Postgres stores JavaScript Object Notation (JSON) payloads and projection
views as jsonb. Spacing and key order can change. The decoded value is the
same. A JSON payload that contains a null character is stored as bytes and
comes back unchanged. SQL Server stores payload bytes as they were written.

## Related

- [Backend selection](BACKEND_SELECTION.md)
- [Postgres partitions and tenants](POSTGRES_PARTITIONS.md)
- [Postgres event store](packages/eventsourcing-postgres.md)
- [SQL Server event store](packages/eventsourcing-sqlserver.md)
