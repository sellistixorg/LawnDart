using System.Collections.Concurrent;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.Outbox;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

public sealed class PostgresContractBackend : IContractBackend
{
    private readonly string _connectionString;
    private readonly ConcurrentDictionary<string, Task> _ready = new();

    public PostgresContractBackend(string connectionString)
        => _connectionString = connectionString;

    public string Name => "Postgres";

    public ContractCapabilities Capabilities { get; } = new()
    {
        EnforcesEventIdAcrossTenants = false,
        StreamIdsCaseInsensitive = false,
        StreamIdPrefixCaseInsensitive = false,
        ChecksTenantIdOnAppend = true,
        AppendWritesOutboxRows = false,
        SecondInstanceSeesCommittedEvents = true,
        ParallelAppendCanConflict = false,
        DuplicateStreamVersionIncludesDatabaseError = false,
        CanPauseBeforeCommit = true,
        CanDropCommitReply = true,
        CanFailCommit = true,
        CanKillConnection = true,
        HasReadHorizon = true,
        UnknownEventNameMarksQueryUnresolved = true,
        PreservesExactJsonText = false,
        RunsLiveSubscriptionTests = false,
        HasOutboxStore = false,
        HasSnapshotStore = false,
        HasInboxStore = false,
        HasProjectionStores = false
    };

    public async Task<ContractEventStore> OpenEventStoreAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        if (options.EnableOutbox)
        {
            throw new InvalidOperationException(
                "EnableOutbox is not available on the Postgres event store yet. Leave EnableOutbox false.");
        }

        var logger = new ListLogger<PostgresEventStore>();
        var storeOptions = new PostgresEventStoreOptions
        {
            ConnectionString = _connectionString,
            SchemaName = options.SchemaName,
            ContextName = options.SchemaName,
            RequireTenantId = options.RequireTenantId,
            ValidateNames = options.ValidateNames,
            EnableStreamRegistry = options.EnableRegistry,
            SubscriptionPollInterval = options.SubscriptionPollInterval ?? TimeSpan.FromMilliseconds(50),
            SubscriptionBatchSize = options.SubscriptionBatchSize ?? 500,
            SubscriptionChannelCapacity = options.SubscriptionChannelCapacity ?? 10_000
        };
        if (options.ReadHorizonGracePeriod is { } grace)
            storeOptions.ReadHorizonGracePeriod = grace;

        var store = new PostgresEventStore(_connectionString, serializer: null, storeOptions, logger);
        store.PublishNewEventsSignal = options.PublishNewEventsSignal;
        await _ready.GetOrAdd(
            _connectionString + "|" + options.SchemaName,
            _ => store.InitializeSchemaAsync(cancellationToken)).ConfigureAwait(false);

        return new ContractEventStore
        {
            SchemaName = options.SchemaName,
            Store = store,
            Log = store,
            Subscriptions = store,
            Signals = store,
            Hooks = new PostgresContractHooks(store, _connectionString),
            Outbox = new InMemoryOutboxWriter(options.OutboxClaimLease),
            ReadLogEntries = () => logger.Entries,
            SetPublishNewEventsSignal = value => store.PublishNewEventsSignal = value
        };
    }

    public Task<IOutboxWriter> OpenOutboxAsync(ContractStoreOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("The Postgres outbox is a later release.");

    public Task<ContractInbox> OpenInboxAsync(ContractStoreOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("The Postgres inbox is a later release.");

    public Task<ContractSnapshots> OpenSnapshotsAsync(ContractStoreOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("Postgres snapshots are a later release.");

    public Task<ContractProjectionStores> OpenProjectionStoresAsync(ContractStoreOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("Postgres projection stores are a later release.");
}

sealed class PostgresContractHooks : IContractHooks
{
    private readonly PostgresEventStore _store;
    private readonly string _connectionString;
    private int _commits;

    public PostgresContractHooks(PostgresEventStore store, string connectionString)
    {
        _store = store;
        _connectionString = connectionString;
    }

    public bool CanPauseBeforeCommit => true;

    public bool CanDropCommitReply => true;

    public bool CanFailCommit => true;

    public bool CanKillConnection => true;

    public bool CanBreakLiveReads => false;

    public int CommitAttempts => Volatile.Read(ref _commits);

    public void PauseNextAppendBeforeCommit(TaskCompletionSource entered, TaskCompletionSource release)
    {
        var gates = 0;
        _store.BeforeAppendCommit = async ct =>
        {
            if (Interlocked.Increment(ref gates) != 1)
                return;
            entered.TrySetResult();
            await release.Task.WaitAsync(ct).ConfigureAwait(false);
        };
    }

    public void DropNextCommitReply()
    {
        _store.CommitHook = async (transaction, ct) =>
        {
            Interlocked.Increment(ref _commits);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            throw new InvalidOperationException("commit reply lost");
        };
    }

    public void FailNextCommit()
    {
        _store.CommitHook = (_, _) =>
        {
            Interlocked.Increment(ref _commits);
            throw new InvalidOperationException("commit did not finish");
        };
    }

    public async Task KillOpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var ids = new List<int>();
        await using (var find = new NpgsqlCommand(
            """
            SELECT pid
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND usename = current_user
              AND xact_start IS NOT NULL
            """,
            connection))
        {
            await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                ids.Add(reader.GetInt32(0));
        }

        if (ids.Count == 0)
            throw new InvalidOperationException("No open append connection to kill.");

        foreach (var id in ids)
        {
            await using var kill = new NpgsqlCommand("SELECT pg_terminate_backend(@pid)", connection);
            kill.Parameters.AddWithValue("pid", id);
            await kill.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public Task BreakLiveReadsAsync(CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("This backend cannot break a live read.");
}
