using LawnDart.EventSourcing.EventStore;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventSourcing.Snapshots;
using LawnDart.Outbox;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

public sealed class InMemoryContractBackend : IContractBackend
{
    public string Name => "in-memory";

    public ContractCapabilities Capabilities { get; } = new()
    {
        EnforcesEventIdAcrossTenants = true,
        StreamIdsCaseInsensitive = false,
        AppendWritesOutboxRows = false,
        SecondInstanceSeesCommittedEvents = false,
        InboxMarksSurviveASecondInstance = false,
        ExposesStoredInboxKey = false,
        CanPurgeExpiredInboxRows = false,
        StoresDcbSnapshotLoadTags = false,
        ParallelAppendCanConflict = false,
        DuplicateStreamVersionIncludesDatabaseError = false,
        CanPauseBeforeCommit = false,
        CanDropCommitReply = false,
        CanFailCommit = false,
        CanKillConnection = false,
        CanBreakLiveReads = false,
        CanCorruptDcbSnapshot = false,
        HasReadHorizon = false,
        UnknownEventNameMarksQueryUnresolved = false
    };

    public Task<ContractEventStore> OpenEventStoreAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var storeOptions = new InMemoryEventStoreOptions
        {
            SubscriptionPollInterval = options.SubscriptionPollInterval ?? TimeSpan.FromMilliseconds(50),
            SubscriptionChannelCapacity = options.SubscriptionChannelCapacity ?? 10_000,
            RequireTenantId = options.RequireTenantId,
            ValidateNames = options.ValidateNames
        };
        var store = new InMemoryEventStore(
            enableRegistry: options.EnableRegistry,
            contextName: options.SchemaName,
            options: storeOptions,
            session: options.CreateSession());
        store.PublishNewEventsSignal = options.PublishNewEventsSignal;
        var opened = new ContractEventStore
        {
            SchemaName = options.SchemaName,
            Store = store,
            Log = store,
            Subscriptions = store,
            Signals = store,
            Hooks = UnavailableHooks.Instance,
            Outbox = new InMemoryOutboxWriter(options.OutboxClaimLease),
            ReadLogEntries = () => [],
            SetPublishNewEventsSignal = value => store.PublishNewEventsSignal = value
        };
        return Task.FromResult(opened);
    }

    public Task<IOutboxWriter> OpenOutboxAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IOutboxWriter>(new InMemoryOutboxWriter(options.OutboxClaimLease));

    public Task<ContractInbox> OpenInboxAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var messaging = new MessagingOptions
        {
            InboxClaimLease = options.InboxClaimLease ?? TimeSpan.FromSeconds(30),
            InboxDeduplicationWindow = options.InboxDeduplicationWindow ?? TimeSpan.FromHours(24)
        };
        var store = new InMemoryInboxStore(Options.Create(messaging));
        return Task.FromResult(new ContractInbox { Store = store });
    }

    public Task<ContractSnapshots> OpenSnapshotsAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var store = new InMemoryEventStore(contextName: options.SchemaName, options: new InMemoryEventStoreOptions
        {
            SubscriptionPollInterval = TimeSpan.FromMilliseconds(50),
            RequireTenantId = options.RequireTenantId,
            ValidateNames = options.ValidateNames
        });
        var snapshots = new InMemorySnapshotStore();
        return Task.FromResult(new ContractSnapshots
        {
            Store = store,
            Snapshots = snapshots,
            DcbSnapshots = snapshots
        });
    }

    public Task<ContractProjectionStores> OpenProjectionStoresAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        return Task.FromResult(new ContractProjectionStores
        {
            Views = views,
            Checkpoints = checkpoints
        });
    }
}

public sealed class InMemoryContractFixture
{
    public IContractBackend Backend { get; } = new InMemoryContractBackend();
}

[CollectionDefinition(Name)]
public sealed class InMemoryContractCollection : ICollectionFixture<InMemoryContractFixture>
{
    public const string Name = "InMemoryContract";
}
