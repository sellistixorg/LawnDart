using LawnDart.EventSourcing.Serialization;
using LawnDart.EventStore;
using LawnDart.Outbox;
using Microsoft.Extensions.Logging;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// Real differences between backends. A test branches on these so the difference
/// shows up in the test instead of being hidden. A backend sets only the values
/// that apply to it; options for stores it does not have are ignored.
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

    /// <summary>
    /// Recognizes the database duplicate-key error inside a <see cref="ConcurrencyException"/>.
    /// Used when <see cref="DuplicateStreamVersionIncludesDatabaseError"/> is true.
    /// </summary>
    public Func<Exception, bool>? IsDatabaseDuplicateKeyError { get; init; }

    public bool CanPauseBeforeCommit { get; init; }

    /// <summary>
    /// Appends lock per tag, so a paused append does not hold up an append on other tags.
    /// A store with one lock around every append leaves this false.
    /// </summary>
    public bool AppendLocksPerTag { get; init; }

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
    /// View JSON text round-trips unchanged.
    /// A store that keeps views in jsonb compares the decoded value instead.
    /// </summary>
    public bool PreservesExactViewJson { get; init; } = true;

    /// <summary>
    /// A stream id prefix matches without regard to case.
    /// </summary>
    public bool StreamIdPrefixCaseInsensitive { get; init; } = true;

    /// <summary>The live subscription contract tests run on this backend.</summary>
    public bool RunsLiveSubscriptionTests { get; init; } = true;

    /// <summary>
    /// A committed append in this process can raise the new-events signal twice:
    /// once from the save, and once when this process's listener reads the wake-up.
    /// </summary>
    public bool CommittedAppendMayEchoSignal { get; init; }

    /// <summary>The backend has an outbox store.</summary>
    public bool HasOutboxStore { get; init; } = true;

    /// <summary>The backend has snapshot tables.</summary>
    public bool HasSnapshotStore { get; init; } = true;

    /// <summary>The backend has an inbox store.</summary>
    public bool HasInboxStore { get; init; } = true;

    /// <summary>The backend has view and checkpoint stores.</summary>
    public bool HasProjectionStores { get; init; } = true;

    /// <summary>The store keeps one row per stream tag beside the stream row.</summary>
    public bool HasStreamTagTable { get; init; }

    /// <summary>
    /// The store honors <see cref="ContractStoreOptions.PublishNewEventsSignal"/> set to false:
    /// a commit then leaves the new-events signal alone. False skips the test that needs it.
    /// </summary>
    public bool CanTurnOffNewEventsSignal { get; init; }
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

    /// <summary>
    /// Event types passed to the store before schema setup.
    /// Null keeps the store's default catalog.
    /// </summary>
    public IReadOnlyList<Type>? EventTypes { get; init; }

    /// <summary>
    /// Builds a session for <see cref="EventTypes"/>.
    /// Null when no types were given.
    /// </summary>
    public EventSession? CreateSession()
    {
        if (EventTypes is not { Count: > 0 })
            return null;
        return new EventSession(new JsonEventSerializer(), EventTypeCatalog.Materialize(EventTypes));
    }
}

/// <summary>
/// Counts on a store's new-events signal. A backend that can read them supplies this;
/// the tests that count signals or wait for a parked reader skip without it.
/// </summary>
public interface IContractSignalCounters
{
    /// <summary>How many times the new-events signal has fired.</summary>
    long NotifyCount { get; }

    /// <summary>How many readers are waiting on the new-events signal right now.</summary>
    int WaiterCount { get; }
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

/// <summary>
/// One opened store for a contract test. <see cref="Store"/> and <see cref="Log"/> are
/// required and usually the same object. The rest is optional: a suite that needs a
/// missing part skips with a reason instead of failing.
/// </summary>
public sealed class ContractEventStore
{
    /// <summary>A name that keeps this store's data apart from other tests.</summary>
    public required string SchemaName { get; init; }

    /// <summary>The typed event store under test.</summary>
    public required IEventStore Store { get; init; }

    /// <summary>The raw event log over the same data as <see cref="Store"/>.</summary>
    public required IEventLog Log { get; init; }

    /// <summary>Live subscriptions. Null skips the subscription tests.</summary>
    public IEventStoreSubscriptions? Subscriptions { get; init; }

    /// <summary>The new-events signal. Null skips the tests that wait on it.</summary>
    public ISignalsNewEvents? Signals { get; init; }

    /// <summary>Counts on the new-events signal. Null skips the tests that count signals.</summary>
    public IContractSignalCounters? SignalCounters { get; init; }

    /// <summary>Fault hooks. The default can do none of them.</summary>
    public IContractHooks Hooks { get; init; } = UnavailableHooks.Instance;

    /// <summary>The outbox written with appends, when the store has one.</summary>
    public IOutboxWriter? Outbox { get; init; }

    /// <summary>Log entries the store wrote, for tests that check a warning.</summary>
    public Func<IReadOnlyList<(LogLevel Level, string Message)>> ReadLogEntries { get; init; } = static () => [];

    /// <summary>Turns the new-events signal on or off after the store is open.</summary>
    public Action<bool> SetPublishNewEventsSignal { get; init; } = static _ => { };

    /// <summary>
    /// Marks one stream archived in the store's registry. There is no public call for this,
    /// so the backend supplies it. Null skips the archived stream test.
    /// </summary>
    public Func<string, CancellationToken, Task>? MarkStreamArchivedAsync { get; init; }
}

/// <summary>
/// The one thing a store supplies to run the contract suites: a way to open a fresh,
/// empty store for each test, and the capabilities that describe it.
/// </summary>
public interface IContractBackend
{
    /// <summary>A short name shown in test output.</summary>
    string Name { get; }

    /// <summary>The store's real differences from the others.</summary>
    ContractCapabilities Capabilities { get; }

    /// <summary>
    /// Opens a store for one test. Use <see cref="ContractStoreOptions.SchemaName"/> to keep
    /// each test's data apart, and honor the options that apply to this store.
    /// </summary>
    Task<ContractEventStore> OpenEventStoreAsync(ContractStoreOptions options, CancellationToken cancellationToken = default);
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
        TimeSpan? inboxDeduplicationWindow = null,
        IReadOnlyList<Type>? eventTypes = null)
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
            InboxDeduplicationWindow = inboxDeduplicationWindow,
            EventTypes = eventTypes
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
