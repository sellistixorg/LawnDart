# Learning path

Eight steps from first command to a production-shaped host.

| Step | Guide | Covers |
|---|---|---|
| 1 | [Concepts](01-concepts.md) | Command / event / state; [CES matrix](../CES_MATRIX.md) |
| 2 | [First aggregate](02-first-aggregate.md) | Library `Book`: events, `BookState`, commands |
| 3 | [Testing your aggregate](03-testing-your-aggregate.md) | InMemory GWT on `Book` (`ThenThrows`) |
| 4 | [Reading state](04-reading-state.md) | `BookState` ≠ `BookCatalogView`; Lightweight |
| 5 | [DCB patterns](05-dcb-patterns.md) | When one aggregate is not enough |
| 6 | [Reactions](06-reactions.md) | `IReactor`, `IEventProcessor`, `ITaskProcessor` |
| 7 | [Testing EDA](07-testing-eda.md) | Reactor harness, inbox deduplication |
| 8 | [Production](08-production.md) | SQL Server, outbox, HTTP auth, schema deploy |

**Prerequisites:** .NET 10 SDK, basic C# records and dependency injection.

Then run [Academy](../QUICKSTART.md).
