using System.Collections.Concurrent;
using LawnDart.EventSourcing.Outbox;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Snapshots;
using LawnDart.Outbox;
using LawnDart.Projections.Lightweight.Postgres;
using Npgsql;
using NpgsqlTypes;

namespace LawnDart.Backends.Contract.Tests;

public sealed class PostgresContractBackend : IContractBackend
{
    private readonly string _connectionString;
    private readonly string? _migrationConnectionString;
    private readonly bool _pollOnly;
    private readonly ConcurrentDictionary<string, Task> _ready = new();

    public PostgresContractBackend(
        string connectionString,
        string? migrationConnectionString = null,
        bool pollOnly = false)
    {
        _connectionString = connectionString;
        _migrationConnectionString = migrationConnectionString;
        _pollOnly = pollOnly;
        Capabilities = new ContractCapabilities
        {
            EnforcesEventIdAcrossTenants = false,
            StreamIdsCaseInsensitive = false,
            StreamIdPrefixCaseInsensitive = false,
            AppendWritesOutboxRows = false,
            SecondInstanceSeesCommittedEvents = true,
            ParallelAppendCanConflict = true,
            DuplicateStreamVersionIncludesDatabaseError = false,
            CanPauseBeforeCommit = true,
            CanDropCommitReply = true,
            CanFailCommit = true,
            CanKillConnection = true,
            HasReadHorizon = true,
            UnknownEventNameMarksQueryUnresolved = true,
            PreservesExactJsonText = false,
            RunsLiveSubscriptionTests = true,
            CanBreakLiveReads = true,
            CommittedAppendMayEchoSignal = !pollOnly,
            HasOutboxStore = false,
            HasSnapshotStore = true,
            StoresDcbSnapshotLoadTags = true,
            CanCorruptDcbSnapshot = true,
            HasInboxStore = false,
            HasProjectionStores = true,
            PreservesExactViewJson = false
        };
    }

    public string Name => "Postgres";

    public ContractCapabilities Capabilities { get; }

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
            MigrationConnectionString = _migrationConnectionString,
            SchemaName = options.SchemaName,
            ContextName = options.SchemaName,
            RequireTenantId = options.RequireTenantId,
            ValidateNames = options.ValidateNames,
            EnableStreamRegistry = options.EnableRegistry,
            BackupPollInterval = options.SubscriptionPollInterval
                ?? (_pollOnly ? TimeSpan.FromMilliseconds(50) : TimeSpan.FromSeconds(30)),
            PollOnlyInterval = options.SubscriptionPollInterval ?? TimeSpan.FromMilliseconds(50),
            SubscriptionBatchSize = options.SubscriptionBatchSize ?? 500,
            SubscriptionChannelCapacity = options.SubscriptionChannelCapacity ?? 10_000
        };
        if (options.ReadHorizonGracePeriod is { } grace)
            storeOptions.ReadHorizonGracePeriod = grace;

        var store = new PostgresEventStore(_connectionString, serializer: null, storeOptions, logger);
        store.PublishNewEventsSignal = options.PublishNewEventsSignal;
        store.ForcePollOnly = _pollOnly;
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
            Hooks = new PostgresContractHooks(store, _connectionString, _migrationConnectionString ?? _connectionString),
            Outbox = new InMemoryOutboxWriter(options.OutboxClaimLease),
            ReadLogEntries = () => logger.Entries,
            SetPublishNewEventsSignal = value => store.PublishNewEventsSignal = value
        };
    }

    public Task<IOutboxWriter> OpenOutboxAsync(ContractStoreOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("The Postgres outbox is a later release.");

    public Task<ContractInbox> OpenInboxAsync(ContractStoreOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("The Postgres inbox is a later release.");

    public async Task<ContractSnapshots> OpenSnapshotsAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var opened = await OpenEventStoreAsync(options, cancellationToken).ConfigureAwait(false);
        var snapshotOptions = new PostgresEventStoreOptions
        {
            ConnectionString = _connectionString,
            MigrationConnectionString = _migrationConnectionString ?? _connectionString,
            SchemaName = options.SchemaName,
            RequireTenantId = options.RequireTenantId,
            ValidateNames = options.ValidateNames
        };
        var snapshots = new PostgresSnapshotStore(snapshotOptions);
        await snapshots.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        return new ContractSnapshots
        {
            Store = opened.Store,
            Snapshots = snapshots,
            DcbSnapshots = snapshots,
            CorruptDcbSnapshotAsync = (id, ct) => CorruptSnapshotAsync(snapshots.QualifiedDcbTable, id, ct),
            ReadDcbLoadTagsAsync = (id, ct) => snapshots.LoadDcbSnapshotTagsAsync(id, ct)
        };
    }

    public async Task<ContractProjectionStores> OpenProjectionStoresAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var storeOptions = new PostgresProjectionStoreOptions
        {
            ConnectionString = _connectionString,
            MigrationConnectionString = _migrationConnectionString ?? _connectionString,
            SchemaName = options.SchemaName,
            EnableSingleRunnerLock = false
        };
        var views = new PostgresViewStore(storeOptions);
        var checkpoints = new PostgresCheckpointStore(storeOptions, views);
        await views.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        await checkpoints.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        return new ContractProjectionStores
        {
            Views = views,
            Checkpoints = checkpoints
        };
    }

    private async Task<int> CorruptSnapshotAsync(string qualifiedTable, string dcbId, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"UPDATE {qualifiedTable} SET state_data = @data WHERE dcb_id = @id",
            connection);
        command.Parameters.Add(new NpgsqlParameter("data", NpgsqlDbType.Bytea) { Value = "not-json"u8.ToArray() });
        command.Parameters.AddWithValue("id", dcbId);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

sealed class PostgresContractHooks : IContractHooks
{
    private readonly PostgresEventStore _store;
    private readonly string _connectionString;
    private readonly string _adminConnectionString;
    private int _commits;

    public PostgresContractHooks(PostgresEventStore store, string connectionString, string adminConnectionString)
    {
        _store = store;
        _connectionString = connectionString;
        _adminConnectionString = adminConnectionString;
    }

    public bool CanPauseBeforeCommit => true;

    public bool CanDropCommitReply => true;

    public bool CanFailCommit => true;

    public bool CanKillConnection => true;

    public bool CanBreakLiveReads => true;

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

    public async Task BreakLiveReadsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_adminConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var rename = new NpgsqlCommand(
            $"""
            SET lock_timeout = '3s';
            ALTER TABLE {_store.QualifiedEventsTable} RENAME COLUMN data TO data_broken;
            """,
            connection)
        {
            CommandTimeout = 15
        };
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await rename.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (PostgresException) when (attempt < 5)
            {
                await Task.Delay(200, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
