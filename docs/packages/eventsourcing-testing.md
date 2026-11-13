# LawnDart.EventSourcing.Testing

Abstract xUnit contract tests for an `IEventStore` and `IEventLog` implementation.
LawnDart's in-memory, SQL Server and Postgres stores run this same suite, so a
store that passes it behaves the way they do. The suite uses only public
LawnDart API, so a store outside this repository runs it the same way.

## What the suite checks

| Suite | What it covers |
|---|---|
| `EventStoreContractTests` | Stream appends and reads, expected versions, metadata, tenant checks |
| `EventLogContractTests` | Raw event log appends, reads and queries |
| `PerEventTagContractTests` | Each event keeps only its own tags, on stream appends, conditional appends, tag queries and log reads |
| `RepeatedTagContractTests` | A tag repeated on one event is stored once, in the order given |
| `TagContractTests` | The aggregate fallback tag format and the unknown tag warning |
| `DcbContractTests` | Dynamic Consistency Boundary appends with an append condition |
| `DcbEntityTagContractTests` | Dynamic Consistency Boundary entities write each event with its own tags |
| `DcbDecisionQueryContractTests` | A decision query loads and fences with OR across items. An event that matches two items is applied once. Two repository races overlap on one item |
| `DcbAppendRaceContractTests` | Conditional appends that start at the same moment: exactly one may win |
| `AggregateEventTagContractTests` | Aggregate saves write each event with its own tags |
| `RepositoryTenantContractTests` | Tenant tags and tenant checks through the repositories |
| `StreamRegistryContractTests` | The stream registry: stream metadata, aggregate types, tags, prefixes |
| `StreamListContractTests` | Stream listing: order, filters, page tokens |
| `SubscriptionContractTests` | Live subscriptions, when the store has them |
| `ReadHorizonContractTests` | The read horizon, when the store has one |
| `OutboxPublicationContractTests` | An append writes an outbox row only for a declared event. 11 tests. A store that does not write rows on append skips them |

## Run it against your store

Reference the package from your store's test project, next to `xunit` and
`Microsoft.NET.Test.Sdk`. The suites use xUnit v2.

Then write one small class that opens your store, and one class per suite.

```csharp
using LawnDart.EventSourcing.Testing;

public sealed class MyStoreBackend : IContractBackend
{
    public string Name => "my-store";

    public ContractCapabilities Capabilities { get; } = new()
    {
        StreamIdsCaseInsensitive = false,
        ParallelAppendCanConflict = true
    };

    public Task<ContractEventStore> OpenEventStoreAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var store = new MyEventStore(
            database: options.SchemaName,
            requireTenantId: options.RequireTenantId,
            session: options.CreateSession());

        return Task.FromResult(new ContractEventStore
        {
            SchemaName = options.SchemaName,
            Store = store,
            Log = store,
            Subscriptions = store,
            Signals = store
        });
    }
}

public sealed class MyStoreFixture
{
    public IContractBackend Backend { get; } = new MyStoreBackend();
}

[CollectionDefinition("MyStore")]
public sealed class MyStoreCollection : ICollectionFixture<MyStoreFixture>;

[Collection("MyStore")]
public sealed class MyStoreEventStoreTests(MyStoreFixture fixture)
    : EventStoreContractTests(fixture.Backend);

[Collection("MyStore")]
public sealed class MyStorePerEventTagTests(MyStoreFixture fixture)
    : PerEventTagContractTests(fixture.Backend);
```

Add one class like the last two for each suite in the table. Run them with
`dotnet test`.

## What your backend must do

- **Keep each test's data apart.** Each test asks for a new
  `ContractStoreOptions.SchemaName`. Use it as a schema, a database, a key
  prefix, or anything else that separates the data. Two opens with the same
  name should reach the same data when your store can do that.
- **Honor the options that apply.** `RequireTenantId`, `ValidateNames` and
  `EnableRegistry` change what the store accepts. If the store has live
  subscriptions, also honor `SubscriptionPollInterval`,
  `SubscriptionBatchSize` and `SubscriptionChannelCapacity`. If it can turn
  off the new-events signal when `PublishNewEventsSignal` is false, set
  `ContractCapabilities.CanTurnOffNewEventsSignal` so the poll-only test runs.
  Options for stores you do not have, such as the outbox, can be ignored.
  A store that writes a row on append sets `AppendWritesOutboxRows` and runs
  `OutboxPublicationContractTests`. When that store opens, call
  `ContractOutboxMaps.For`. With the outbox on and nothing declared, that call
  throws `PublishedEventMap.NothingDeclaredMessage`, which names the context
  and lists its event types. The in-memory, SQL Server, and Postgres stores
  run these 11 tests.
- **Know the test events.** When `options.CreateSession()` returns a session,
  give it to your store so it can read the suite's events back as types.
- **Return both views of one store.** `Store` is your `IEventStore` and `Log`
  is your `IEventLog`. Both must read and write the same data. Several tests
  write through one and read through the other, and expect the same tags.

`Subscriptions`, `Signals`, `SignalCounters`, `Hooks` and
`MarkStreamArchivedAsync` are optional. Leave one out and the tests that need
it are reported as skipped, with the reason. A skipped test is not a pass.

`SignalCounters` reports how many times the new-events signal fired and how
many readers wait on it. With it, the subscription tests also check that a
commit fires the signal once and that a failed append does not fire it.
Without it, those tests still check the signal position, and the one test
that has to wait for a parked reader is skipped.

## Capabilities

`ContractCapabilities` lists the real differences between stores, for example
whether stream ids ignore case or whether two parallel appends to one stream
can conflict. A test reads these values and checks the behavior your store
says it has. Check each value against your store. Most default to false. A few default
to true, such as `PreservesExactJsonText` and `StreamIdPrefixCaseInsensitive`.

## Related

- [Package map](README.md)
- [EventSourcing](eventsourcing.md)
- [Backend differences](../BACKEND_DIFFERENCES.md)
- [Tagging](../TAGGING.md)
