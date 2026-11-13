# LawnDart.EventSourcing.Testing

Abstract xUnit contract tests for an `IEventStore` and `IEventLog` implementation.

Implement `IContractBackend` in your store's test project so each test can open
a fresh store, then inherit the suites you want to run, such as
`EventStoreContractTests`, `PerEventTagContractTests`, `RepeatedTagContractTests`
and `DcbAppendRaceContractTests`. LawnDart's in-memory, SQL Server and Postgres
stores run the same suites.

`OutboxPublicationContractTests` checks that an append writes a row only for a
declared event. `ContractOutboxMaps.For` builds that map. With the outbox on and
nothing declared, it throws the same message the built-in stores throw.

See [EventSourcing.Testing](https://github.com/sellistixorg/LawnDart/blob/main/docs/packages/eventsourcing-testing.md).
