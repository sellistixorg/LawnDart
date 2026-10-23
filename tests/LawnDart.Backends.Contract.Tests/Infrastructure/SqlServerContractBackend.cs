using System.Collections.Concurrent;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventSourcing.SqlServer.Outbox;
using LawnDart.EventSourcing.SqlServer.Snapshots;
using LawnDart.Messaging;
using LawnDart.Messaging.SqlServer;
using LawnDart.Outbox;
using LawnDart.Projections.Lightweight.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

public sealed class SqlServerContractBackend : IContractBackend
{
    private readonly string _connectionString;
    private readonly ConcurrentDictionary<string, Task> _ready = new();

    public SqlServerContractBackend(string connectionString)
        => _connectionString = connectionString;

    public string Name => "SQL Server";

    public ContractCapabilities Capabilities { get; } = new()
    {
        EnforcesEventIdAcrossTenants = true,
        StreamIdsCaseInsensitive = true,
        AppendWritesOutboxRows = true,
        SecondInstanceSeesCommittedEvents = true,
        InboxMarksSurviveASecondInstance = true,
        ExposesStoredInboxKey = true,
        CanPurgeExpiredInboxRows = true,
        StoresDcbSnapshotLoadTags = true,
        ParallelAppendCanConflict = true,
        DuplicateStreamVersionIncludesDatabaseError = true,
        CanPauseBeforeCommit = true,
        CanDropCommitReply = true,
        CanFailCommit = true,
        CanKillConnection = true,
        CanBreakLiveReads = true,
        CanCorruptDcbSnapshot = true,
        HasReadHorizon = true,
        UnknownEventNameMarksQueryUnresolved = true,
        HasStreamTagTable = true
    };

    public async Task<ContractEventStore> OpenEventStoreAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var connectionString = options.ReadCommittedSnapshot is { } snapshot
            ? await CreateDatabaseAsync(snapshot, cancellationToken).ConfigureAwait(false)
            : _connectionString;
        var schema = options.SchemaName;
        var logger = new ListLogger<SqlServerEventStore>();
        var storeOptions = new SqlServerEventStoreOptions
        {
            RequireTenantId = options.RequireTenantId,
            ValidateNames = options.ValidateNames,
            SchemaName = schema,
            ContextName = schema,
            EnableOutbox = options.EnableOutbox,
            OutboxTableName = "Outbox",
            SqlSubscriptionPollInterval = options.SubscriptionPollInterval ?? TimeSpan.FromMilliseconds(50),
            SqlSubscriptionBatchSize = options.SubscriptionBatchSize ?? 500,
            SubscriptionChannelCapacity = options.SubscriptionChannelCapacity ?? 10_000
        };
        if (options.ReadHorizonGracePeriod is { } grace)
            storeOptions.ReadHorizonGracePeriod = grace;

        var outbox = new SqlServerOutboxWriter(
            connectionString,
            "Outbox",
            schemaName: schema,
            claimLease: options.OutboxClaimLease);
        if (options.EnableOutbox || options.IncludeOutboxReader)
        {
            await _ready.GetOrAdd(
                connectionString + "|outbox|" + schema,
                _ => outbox.InitializeSchemaAsync(cancellationToken)).ConfigureAwait(false);
        }

        var store = new SqlServerEventStore(
            connectionString,
            serializer: null,
            tableName: "Events",
            registryTableName: "Streams",
            enableRegistry: options.EnableRegistry,
            options: storeOptions,
            outboxWriter: options.EnableOutbox ? outbox : null,
            logger: logger,
            session: options.CreateSession());
        store.PublishNewEventsSignal = options.PublishNewEventsSignal;

        await _ready.GetOrAdd(
            connectionString + "|events|" + schema,
            _ => store.InitializeSchemaAsync(cancellationToken)).ConfigureAwait(false);

        return new ContractEventStore
        {
            SchemaName = schema,
            Store = store,
            Log = store,
            Subscriptions = store,
            Signals = store,
            Hooks = new SqlServerContractHooks(store, connectionString, schema),
            Outbox = outbox,
            ReadLogEntries = () => logger.Entries,
            SetPublishNewEventsSignal = value => store.PublishNewEventsSignal = value
        };
    }

    public async Task<IOutboxWriter> OpenOutboxAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var writer = new SqlServerOutboxWriter(
            _connectionString,
            "Outbox",
            schemaName: options.SchemaName,
            claimLease: options.OutboxClaimLease);
        await _ready.GetOrAdd(
            _connectionString + "|outbox|" + options.SchemaName,
            _ => writer.InitializeSchemaAsync(cancellationToken)).ConfigureAwait(false);
        return writer;
    }

    public async Task<ContractInbox> OpenInboxAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var messaging = new MessagingOptions
        {
            InboxClaimLease = options.InboxClaimLease ?? TimeSpan.FromSeconds(30),
            InboxDeduplicationWindow = options.InboxDeduplicationWindow ?? TimeSpan.FromHours(24)
        };
        var store = CreateInbox(options.SchemaName, messaging);
        await store.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        var table = store.QualifiedTableName;
        return new ContractInbox
        {
            Store = store,
            PurgeExpiredAsync = store.PurgeExpiredAsync,
            CountRowsAsync = ct => CountRowsAsync(table, ct),
            ReadSingleKeyAsync = ct => ReadSingleKeyAsync(table, ct),
            OpenSecondInstanceAsync = async ct =>
            {
                var second = CreateInbox(options.SchemaName, messaging);
                await second.InitializeSchemaAsync(ct).ConfigureAwait(false);
                return second;
            }
        };
    }

    public async Task<ContractSnapshots> OpenSnapshotsAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var opened = await OpenEventStoreAsync(options, cancellationToken).ConfigureAwait(false);
        var snapshotOptions = new SqlServerEventStoreOptions
        {
            ConnectionString = options.ReadCommittedSnapshot is null ? _connectionString : null,
            SchemaName = options.SchemaName,
            RequireTenantId = options.RequireTenantId
        };
        // The event store already created the snapshot tables during schema init.
        // A dedicated database uses that connection string, which the event store holds.
        var connectionString = ConnectionStringOf(opened);
        snapshotOptions.ConnectionString = connectionString;
        var snapshots = new SqlServerSnapshotStore(snapshotOptions);
        await snapshots.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        return new ContractSnapshots
        {
            Store = opened.Store,
            Snapshots = snapshots,
            DcbSnapshots = snapshots,
            CorruptDcbSnapshotAsync = (id, ct) => CorruptAsync(snapshots.QualifiedDcbTable, connectionString, id, ct),
            ReadDcbLoadTagsAsync = (id, ct) => snapshots.LoadDcbSnapshotTagsAsync(id, ct)
        };
    }

    public async Task<ContractProjectionStores> OpenProjectionStoresAsync(
        ContractStoreOptions options,
        CancellationToken cancellationToken = default)
    {
        var views = new SqlViewStore(_connectionString, schemaName: options.SchemaName);
        var checkpoints = new SqlCheckpointStore(_connectionString, views, schemaName: options.SchemaName);
        await views.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        await checkpoints.InitializeSchemaAsync(cancellationToken).ConfigureAwait(false);
        return new ContractProjectionStores
        {
            Views = views,
            Checkpoints = checkpoints
        };
    }

    private SqlServerInboxStore CreateInbox(string schema, MessagingOptions messaging)
        => new(
            Options.Create(new SqlServerInboxStoreOptions
            {
                ConnectionString = _connectionString,
                SchemaName = schema,
                TableName = "Inbox"
            }),
            Options.Create(messaging));

    private async Task<string> CreateDatabaseAsync(bool readCommittedSnapshot, CancellationToken cancellationToken)
    {
        var name = "db" + Guid.NewGuid().ToString("N");
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var flag = readCommittedSnapshot ? "ON" : "OFF";
        await using var command = new SqlCommand(
            $"CREATE DATABASE [{name}]; ALTER DATABASE [{name}] SET READ_COMMITTED_SNAPSHOT {flag} WITH ROLLBACK IMMEDIATE;",
            connection);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        var builder = new SqlConnectionStringBuilder(_connectionString) { InitialCatalog = name };
        return builder.ConnectionString;
    }

    private static string ConnectionStringOf(ContractEventStore opened)
        => ((SqlServerContractHooks)opened.Hooks).ConnectionString;

    private async Task<int> CountRowsAsync(string qualifiedTable, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM {qualifiedTable}", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
    }

    private async Task<string?> ReadSingleKeyAsync(string qualifiedTable, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand($"SELECT TOP (1) MessageId FROM {qualifiedTable}", connection);
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is null or DBNull ? null : (string)value;
    }

    private static async Task<int> CorruptAsync(
        string qualifiedTable,
        string connectionString,
        string dcbId,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand(
            $"UPDATE {qualifiedTable} SET [StateData] = N'not-json' WHERE [DcbId] = @Id",
            connection);
        var parameter = command.Parameters.Add("@Id", System.Data.SqlDbType.Char, 64);
        parameter.Value = dcbId;
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

sealed class SqlServerContractHooks : IContractHooks
{
    private readonly SqlServerEventStore _store;
    private readonly string _connectionString;
    private readonly string _schema;
    private int _commits;

    public SqlServerContractHooks(SqlServerEventStore store, string connectionString, string schema)
    {
        _store = store;
        _connectionString = connectionString;
        _schema = schema;
    }

    public string ConnectionString => _connectionString;

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
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var ids = new List<short>();
        await using (var find = new SqlCommand(
            """
            SELECT session_id
            FROM sys.dm_exec_sessions
            WHERE database_id = DB_ID()
              AND session_id <> @@SPID
              AND open_transaction_count > 0
            """,
            connection))
        {
            await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                ids.Add(reader.GetInt16(0));
        }

        if (ids.Count == 0)
            throw new InvalidOperationException("No open append connection to kill.");

        foreach (var id in ids)
        {
            await using var kill = new SqlCommand($"KILL {id}", connection);
            await kill.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task BreakLiveReadsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var rename = new SqlCommand(
            $"""
            SET LOCK_TIMEOUT 3000;
            EXEC sp_rename '{_schema}.Events.EventData', 'EventData_x', 'COLUMN';
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
            catch (SqlException) when (attempt < 5)
            {
                await Task.Delay(200, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
