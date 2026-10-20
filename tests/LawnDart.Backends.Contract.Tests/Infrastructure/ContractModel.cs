using LawnDart.EventStore;
using LawnDart.Outbox;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Storage;
using LawnDart.Snapshots;
using Microsoft.Extensions.Logging;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Real differences between backends. A test branches on these so the difference
/// shows up in the test instead of being hidden.
/// </summary>
public sealed class ContractCapabilities
{
    /// <summary>The same event id is rejected on a second tenant in the same store.</summary>
    public bool EnforcesEventIdAcrossTenants { get; init; }

    /// <summary>Stream id <c>Order</c> and <c>order</c> are the same stream.</summary>
    public bool StreamIdsCaseInsensitive { get; init; }

    /// <summary>An append writes outbox rows when that option is on.</summary>
    public bool AppendWritesOutboxRows { get; init; }

    /// <summary>A second store object on the same schema sees committed events and can refresh its event-name cache.</summary>
    public bool SecondInstanceSeesCommittedEvents { get; init; }

    /// <summary>A second inbox object on the same schema sees marks from the first.</summary>
    public bool InboxMarksSurviveASecondInstance { get; init; }

    /// <summary>The test can read the one stored inbox key back.</summary>
    public bool ExposesStoredInboxKey { get; init; }

    /// <summary>The inbox can delete expired rows in one call.</summary>
    public bool CanPurgeExpiredInboxRows { get; init; }

    /// <summary>A DCB snapshot keeps the plaintext load tags beside the hashed key.</summary>
    public bool StoresDcbSnapshotLoadTags { get; init; }

    /// <summary>Parallel appends to one stream can lose and throw <see cref="ConcurrencyException"/>.</summary>
    public bool ParallelAppendCanConflict { get; init; }

    /// <summary>A conflicting parallel append's inner error is the database duplicate-key error.</summary>
    public bool DuplicateStreamVersionIncludesDatabaseError { get; init; }

    public bool CanPauseBeforeCommit { get; init; }

    public bool CanDropCommitReply { get; init; }

    public bool CanFailCommit { get; init; }

    public bool CanKillConnection { get; init; }

    public bool CanBreakLiveReads { get; init; }

    public bool CanCorruptDcbSnapshot { get; init; }

    /// <summary>An open append holds the caught-up sequence below a later commit.</summary>
    public bool HasReadHorizon { get; init; }

    /// <summary>
    /// A query for an event name the store has never seen sets
    /// <c>TypesResolved</c> to false. A store with no event-name table leaves it true.
    /// </summary>
    public bool UnknownEventNameMarksQueryUnresolved { get; init; }

    /// <summary>
    /// JSON payload and metadata bytes round-trip unchanged.
    /// A store that keeps JSON in jsonb compares the decoded value instead.
    /// </summary>
    public bool PreservesExactJsonText { get; init; } = true;

    /// <summary>
    /// A stream id prefix matches without regard to case.
    /// </summary>
    public bool StreamIdPrefixCaseInsensitive { get; init; } = true;

    /// <summary>The live subscription contract tests run on this backend.</summary>
    public bool RunsLiveSubscriptionTests { get; init; } = true;

    /// <summary>The backend has an outbox store.</summary>
    public bool HasOutboxStore { get; init; } = true;

    /// <summary>The backend has snapshot tables.</summary>
    public bool HasSnapshotStore { get; init; } = true;

    /// <summary>The backend has an inbox store.</summary>
    public bool HasInboxStore { get; init; } = true;

    /// <summary>The backend has view and checkpoint stores.</summary>
    public bool HasProjectionStores { get; init; } = true;
}

public sealed class ContractStoreOptions
{
    public string SchemaName { get; init; } = "dbo";

    public bool RequireTenantId { get; init; }

    public bool ValidateNames { get; init; } = true;

    public bool EnableRegistry { get; init; } = true;

    public bool EnableOutbox { get; init; }

    public bool IncludeOutboxReader { get; init; }

    public TimeSpan? ReadHorizonGracePeriod { get; init; }

    public int? SubscriptionBatchSize { get; init; }

    public int? SubscriptionChannelCapacity { get; init; }

    public TimeSpan? SubscriptionPollInterval { get; init; }

    public bool PublishNewEventsSignal { get; init; } = true;

    /// <summary>
    /// When set, the backend opens a new database with read-committed snapshot on or off.
    /// Null uses the shared database.
    /// </summary>
    public bool? ReadCommittedSnapshot { get; init; }

    public TimeSpan? OutboxClaimLease { get; init; }

    public TimeSpan? InboxClaimLease { get; init; }

    public TimeSpan? InboxDeduplicationWindow { get; init; }
}

public interface IContractHooks
{
    bool CanPauseBeforeCommit { get; }

    bool CanDropCommitReply { get; }

    bool CanFailCommit { get; }

    bool CanKillConnection { get; }

    bool CanBreakLiveReads { get; }

    int CommitAttempts { get; }

    void PauseNextAppendBeforeCommit(TaskCompletionSource entered, TaskCompletionSource release);

    void DropNextCommitReply();

    void FailNextCommit();

    Task KillOpenConnectionAsync(CancellationToken cancellationToken = default);

    Task BreakLiveReadsAsync(CancellationToken cancellationToken = default);
}

public sealed class ContractEventStore
{
    public required string SchemaName { get; init; }

    public required IEventStore Store { get; init; }

    public required IEventLog Log { get; init; }

    public required IEventStoreSubscriptions Subscriptions { get; init; }

    public required ISignalsNewEvents Signals { get; init; }

    public required IContractHooks Hooks { get; init; }

    public required IOutboxWriter Outbox { get; init; }

    public required Func<IReadOnlyList<(LogLevel Level, string Message)>> ReadLogEntries { get; init; }

    public required Action<bool> SetPublishNewEventsSignal { get; init; }
}

public sealed class ContractInbox
{
    public required LawnDart.Messaging.IInboxStore Store { get; init; }

    public Func<CancellationToken, Task<int>>? PurgeExpiredAsync { get; init; }

    public Func<CancellationToken, Task<int>>? CountRowsAsync { get; init; }

    public Func<CancellationToken, Task<string?>>? ReadSingleKeyAsync { get; init; }

    public Func<CancellationToken, Task<LawnDart.Messaging.IInboxStore>>? OpenSecondInstanceAsync { get; init; }
}

public sealed class ContractSnapshots
{
    public required IEventStore Store { get; init; }

    public required ISnapshotStore Snapshots { get; init; }

    public required IDcbSnapshotStore DcbSnapshots { get; init; }

    public Func<string, CancellationToken, Task<int>>? CorruptDcbSnapshotAsync { get; init; }

    public Func<string, CancellationToken, Task<IReadOnlyList<string>?>>? ReadDcbLoadTagsAsync { get; init; }
}

public sealed class ContractProjectionStores
{
    public required IViewStore Views { get; init; }

    public required ICheckpointStore Checkpoints { get; init; }
}

public interface IContractBackend
{
    string Name { get; }

    ContractCapabilities Capabilities { get; }

    Task<ContractEventStore> OpenEventStoreAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);

    Task<IOutboxWriter> OpenOutboxAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);

    Task<ContractInbox> OpenInboxAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);

    Task<ContractSnapshots> OpenSnapshotsAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);

    Task<ContractProjectionStores> OpenProjectionStoresAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);
}

public static class ContractStores
{
    public static ContractStoreOptions Isolated(
        bool requireTenantId = false,
        bool enableOutbox = false,
        bool includeOutboxReader = false,
        TimeSpan? readHorizonGracePeriod = null,
        int? subscriptionBatchSize = null,
        int? subscriptionChannelCapacity = null,
        TimeSpan? subscriptionPollInterval = null,
        bool publishNewEventsSignal = true,
        bool? readCommittedSnapshot = null,
        bool validateNames = true,
        TimeSpan? outboxClaimLease = null,
        TimeSpan? inboxClaimLease = null,
        TimeSpan? inboxDeduplicationWindow = null)
        => new()
        {
            SchemaName = "c" + Guid.NewGuid().ToString("N"),
            RequireTenantId = requireTenantId,
            ValidateNames = validateNames,
            EnableOutbox = enableOutbox,
            IncludeOutboxReader = includeOutboxReader || enableOutbox,
            ReadHorizonGracePeriod = readHorizonGracePeriod,
            SubscriptionBatchSize = subscriptionBatchSize,
            SubscriptionChannelCapacity = subscriptionChannelCapacity,
            SubscriptionPollInterval = subscriptionPollInterval,
            PublishNewEventsSignal = publishNewEventsSignal,
            ReadCommittedSnapshot = readCommittedSnapshot,
            OutboxClaimLease = outboxClaimLease,
            InboxClaimLease = inboxClaimLease,
            InboxDeduplicationWindow = inboxDeduplicationWindow
        };
}

public sealed class ListLogger<T> : ILogger<T>
{
    private readonly object _gate = new();
    private readonly List<(LogLevel Level, string Message)> _entries = [];

    public IReadOnlyList<(LogLevel Level, string Message)> Entries
    {
        get
        {
            lock (_gate)
                return _entries.ToArray();
        }
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        lock (_gate)
            _entries.Add((logLevel, formatter(state, exception)));
    }
}

public sealed class UnavailableHooks : IContractHooks
{
    public static UnavailableHooks Instance { get; } = new();

    public bool CanPauseBeforeCommit => false;

    public bool CanDropCommitReply => false;

    public bool CanFailCommit => false;

    public bool CanKillConnection => false;

    public bool CanBreakLiveReads => false;

    public int CommitAttempts => 0;

    public void PauseNextAppendBeforeCommit(TaskCompletionSource entered, TaskCompletionSource release)
        => throw new InvalidOperationException("This backend cannot pause an append before commit.");

    public void DropNextCommitReply()
        => throw new InvalidOperationException("This backend cannot drop the commit reply.");

    public void FailNextCommit()
        => throw new InvalidOperationException("This backend cannot fail a commit after it was attempted.");

    public Task KillOpenConnectionAsync(CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("This backend cannot kill the append connection.");

    public Task BreakLiveReadsAsync(CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("This backend cannot break a live read.");
}
