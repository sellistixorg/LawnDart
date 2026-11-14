# LawnDart.EventSourcing.Testing

Abstract xUnit contract tests for an `IEventStore` and `IEventLog` implementation.

Implement `IContractBackend` in your store's test project so each test can open
a fresh store, then inherit the suites you want to run, such as
`EventStoreContractTests`, `PerEventTagContractTests`, `RepeatedTagContractTests`
and `DcbAppendRaceContractTests`. LawnDart's in-memory, SQL Server and Postgres
stores run the same suites.

`OutboxPublicationContractTests` is 11 tests. An append writes a row only for a
declared event. `ContractOutboxMaps.For` builds that map. With the outbox on and
nothing declared, it throws `PublishedEventMap.NothingDeclaredMessage`, the same
text the in-memory, SQL Server, and Postgres stores throw.

See [EventSourcing.Testing](https://github.com/sellistixorg/LawnDart/blob/main/docs/packages/eventsourcing-testing.md).
