using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventStore;
using LawnDart.Serialization;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// Postgres <see cref="IEventLog"/>. Typed <see cref="IEventStore"/> methods
/// forward to <see cref="EventStoreAdapter"/>.
/// </summary>
public sealed partial class PostgresEventStore : IEventStore, IEventStoreSubscriptions, IEventLog, IEventLogSubscriptions, ISignalsNewEvents
{
    /// <summary>Format written into <c>schema_info</c>. Any other number is a drop and recreate.</summary>
    public const int CurrentSchemaFormat = PostgresSchema.CurrentFormat;

    /// <summary>Stream ids are capped at 255 characters so a stream can move between backends.</summary>
    public const int StreamIdMaxLength = 255;

    private static readonly ConcurrentDictionary<string, NpgsqlDataSource> Sources = new(StringComparer.Ordinal);

    private readonly EventSession _session;
    private readonly EventStoreAdapter _adapter;
    private readonly NpgsqlDataSource _dataSource;
    private readonly NpgsqlDataSource _migration;
    private readonly PostgresEventStoreOptions _options;
    private readonly PostgresNames _names;
    private readonly ILogger<PostgresEventStore>? _logger;
    private readonly EventTypeIdCache _eventTypes = new();
    private readonly SemaphoreSlim _typeCacheRefresh = new(1, 1);
    private readonly PostgresReadHorizon _horizon;
    private readonly NewEventsSignal _newEvents = new();
    private readonly ConcurrentDictionary<PostgresSubscriptionHandle, byte> _subscriptions = new();
    private readonly bool _enableRegistry;

    /// <summary>Creates a store. Call <see cref="InitializeSchemaAsync"/> before the first append.</summary>
    public PostgresEventStore(
        string connectionString,
        IEventSerializer? serializer = null,
        PostgresEventStoreOptions? options = null,
        ILogger<PostgresEventStore>? logger = null,
        EventSession? session = null)
    {
        _options = options ?? new PostgresEventStoreOptions { ConnectionString = connectionString };
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            _options.ConnectionString = connectionString;
        _options.EnsureValid();
        _names = new PostgresNames(_options);
        _session = session ?? new EventSession(serializer ?? new JsonEventSerializer(), new WriteThroughEventTypeCatalog());
        _adapter = new EventStoreAdapter(this, _session, this, _options.SubscriptionChannelCapacity);
        _dataSource = Source(_options.ConnectionString!);
        _migration = string.IsNullOrWhiteSpace(_options.MigrationConnectionString)
            ? _dataSource
            : Source(_options.MigrationConnectionString);
        _logger = logger;
        _horizon = new PostgresReadHorizon(_names, _options, logger);
        _enableRegistry = _options.EnableStreamRegistry;
        ContextName = string.IsNullOrWhiteSpace(_options.ContextName) ? "default" : _options.ContextName;
    }

    /// <summary>Bounded context name.</summary>
    public string ContextName { get; }

    /// <summary>In-process wake-up. Raised after a save commits.</summary>
    public NewEventsSignal NewEvents => _newEvents;

    /// <summary>Test seam. A committed save does not raise <see cref="NewEvents"/> when this is false.</summary>
    internal bool PublishNewEventsSignal { get; set; } = true;

    /// <summary>Test seam. Runs inside the append transaction after the rows are written and before commit.</summary>
    internal Func<CancellationToken, Task>? BeforeAppendCommit { get; set; }

    /// <summary>Test seam. Replaces commit. The store does not retry after this runs.</summary>
    internal Func<NpgsqlTransaction, CancellationToken, Task>? CommitHook { get; set; }

    /// <summary>
    /// Test seam. Runs after the append statement is sent. A throw here is a lost
    /// reply: the store reads the event ids back and does not send the statement again.
    /// </summary>
    internal Func<CancellationToken, Task>? AfterAppendStatement { get; set; }

    /// <summary>Quoted events table.</summary>
    internal string QualifiedEventsTable => _names.Events;

    /// <summary>Quoted stream table.</summary>
    internal string QualifiedStreamsTable => _names.Streams;

    /// <summary>Quoted tag table.</summary>
    internal string QualifiedTagsTable => _names.Tags;

    /// <summary>Connection string the store uses for reads and appends.</summary>
    internal string ConnectionString => _options.ConnectionString!;

    /// <summary>Schema name.</summary>
    internal string SchemaName => _names.Schema;

    /// <inheritdoc />
    public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId, long fromVersion = 0, long? toVersion = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
        => _adapter.ReadStreamAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId, long fromVersion = 0, long? toVersion = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
        => _adapter.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public Task<QueryResult> ReadByQueryAsync(
        Query query, long? fromSequencePosition = null, int? limit = null, long? toSequencePosition = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
        => _adapter.ReadByQueryAsync(query, fromSequencePosition, limit, toSequencePosition, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query, long? fromSequencePosition = null, long? toSequencePosition = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
        => _adapter.ReadByQueryStreamAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public Task<AppendResult> AppendAsync(
        string streamId, IEnumerable<IEvent> events, long? expectedVersion = null, LawnDart.Metadata.EventMetadata? metadata = null, IEnumerable<string>? tags = null, CancellationToken cancellationToken = default)
        => _adapter.AppendAsync(streamId, events, expectedVersion, metadata, tags, cancellationToken);

    /// <inheritdoc />
    public Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events, AppendCondition condition, LawnDart.Metadata.EventMetadata? metadata = null, IEnumerable<string>? tags = null, CancellationToken cancellationToken = default)
        => _adapter.AppendAsync(events, condition, metadata, tags, cancellationToken);

    /// <inheritdoc />
    public Task<long> GetMaxSequencePositionAsync(
        Query query, long? fromSequencePosition = null, long? toSequencePosition = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
        => _adapter.GetMaxSequencePositionAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public ISubscriptionHandle Subscribe(string subscriberId, long fromSequence, EventSubscriptionFilter? filter = null, CancellationToken cancellationToken = default)
        => _adapter.Subscribe(subscriberId, fromSequence, filter, cancellationToken);

    /// <summary>Creates the schema when it is missing. A second call with the same bucket count succeeds.</summary>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _migration.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (var gate = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(hashtextextended(@key, 0))", connection, transaction))
        {
            gate.Parameters.AddWithValue("key", _names.Schema + ":init");
            await gate.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var infoExists = await RegclassExistsAsync(connection, transaction, _names.SchemaInfo, cancellationToken).ConfigureAwait(false);
        if (infoExists)
        {
            var found = await ReadStampAsync(connection, transaction, cancellationToken).ConfigureAwait(false)
                ?? throw new IncompatibleEventStoreSchemaException(_names.Schema, null, CurrentSchemaFormat);
            if (found.Format != CurrentSchemaFormat)
                throw new IncompatibleEventStoreSchemaException(_names.Schema, found.Format, CurrentSchemaFormat);
            if (found.PartitionCount != _options.PartitionCount || found.Partitioning != _options.Partitioning)
            {
                throw new InvalidOperationException(
                    $"This schema was created with {found.PartitionCount} hash buckets " +
                    $"and partitioning {(found.Partitioning ? "on" : "off")}. " +
                    $"PartitionCount is {_options.PartitionCount} and partitioning is {(_options.Partitioning ? "on" : "off")}. " +
                    "Drop and recreate the schema to change the bucket layout.");
            }
        }
        else if (await RegclassExistsAsync(connection, transaction, _names.Events, cancellationToken).ConfigureAwait(false))
        {
            throw new IncompatibleEventStoreSchemaException(_names.Schema, null, CurrentSchemaFormat);
        }

        foreach (var statement in PostgresSchema.CreateStatements(_names, _options))
            await ExecuteAsync(connection, transaction, statement, cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, PostgresSchema.StreamIndexes(_names), cancellationToken).ConfigureAwait(false);
        await GrantAppRoleAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        await ReloadEventTypeCacheAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<AppendEvent> events,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));
        EnsureStreamLength(streamId, nameof(streamId));
        var envelopes = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (envelopes.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));

        ValidateStreamTenant(streamId, envelopes);
        var eventIds = EventIdResolver.ResolveBatch(envelopes.Select(e => e.Metadata).ToList());
        return await AppendWithRetry(
            ct => AppendCoreAsync(streamId, envelopes, eventIds, expectedVersion, condition: null, isDcb: false, ct),
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<AppendResult> AppendAsync(
        IEnumerable<AppendEvent> events,
        AppendCondition condition,
        CancellationToken cancellationToken = default)
    {
        var envelopes = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (envelopes.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));
        ArgumentNullException.ThrowIfNull(condition);

        var tenantId = TryReadTenantId(envelopes);
        if (_options.RequireTenantId && string.IsNullOrWhiteSpace(tenantId))
        {
            throw new ArgumentException(
                "TenantId is required in event metadata when RequireTenantId is true for DCB appends.");
        }

        if (_options.RequireTenantId && !string.IsNullOrWhiteSpace(tenantId))
            LawnDart.LawnDartNames.ValidateTenantId(tenantId, "metadata", _options.ValidateNames);

        var streamId = !string.IsNullOrWhiteSpace(tenantId)
            ? $"{tenantId}:dcb:{Guid.NewGuid()}"
            : $"dcb:{Guid.NewGuid()}";
        EnsureStreamLength(streamId, "streamId");
        var eventIds = EventIdResolver.ResolveBatch(envelopes.Select(e => e.Metadata).ToList());
        return await AppendWithRetry(
            ct => AppendCoreAsync(streamId, envelopes, eventIds, expectedVersion: null, condition, isDcb: true, ct),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes one batch and rolls it back. Used by the stress test to prove a
    /// rolled-back append is not read.
    /// </summary>
    internal async Task AppendAndRollbackAsync(
        string streamId,
        IReadOnlyList<AppendEvent> events,
        CancellationToken cancellationToken = default)
    {
        await EnsureEventTypeIdsAsync(events, cancellationToken).ConfigureAwait(false);
        var ids = EventIdResolver.ResolveBatch(events.Select(e => e.Metadata).ToList());
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteAppendAsync(connection, transaction, streamId, events, ids, null, null, false, cancellationToken).ConfigureAwait(false);
        await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<AppendResult> AppendCoreAsync(
        string streamId,
        IReadOnlyList<AppendEvent> envelopes,
        IReadOnlyList<Guid> eventIds,
        long? expectedVersion,
        AppendCondition? condition,
        bool isDcb,
        CancellationToken cancellationToken)
    {
        await EnsureEventTypeIdsAsync(envelopes, cancellationToken).ConfigureAwait(false);
        if (condition is not null)
            await EnsureQueryTypesResolvedAsync(condition.FailIfEventsMatch, cancellationToken).ConfigureAwait(false);

        var useTransaction = BeforeAppendCommit is not null || CommitHook is not null;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        if (!useTransaction)
        {
            AppendWrite written;
            try
            {
                written = await ExecuteAppendAsync(
                    connection, transaction: null, streamId, envelopes, eventIds, expectedVersion, condition, isDcb, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (AppendStatementSentException ex)
            {
                return await RecoverSentAppendAsync(streamId, eventIds, isDcb, ex, cancellationToken).ConfigureAwait(false);
            }

            var committed = new AppendResult(written.Positions, null, isDcb ? -1 : written.LastVersion);
            Signal(committed);
            return committed;
        }

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        AppendWrite writtenRows;
        try
        {
            writtenRows = await ExecuteAppendAsync(
                connection, transaction, streamId, envelopes, eventIds, expectedVersion, condition, isDcb, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            await RollbackQuietlyAsync(transaction, cancellationToken).ConfigureAwait(false);
            throw;
        }

        if (BeforeAppendCommit is { } pause)
            await pause(cancellationToken).ConfigureAwait(false);

        try
        {
            if (CommitHook is { } hook)
                await hook(transaction, cancellationToken).ConfigureAwait(false);
            else
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var landed = await TryReadCommittedAppendAsync(streamId, eventIds, cancellationToken).ConfigureAwait(false);
            if (landed is not null)
            {
                Signal(landed);
                return landed;
            }

            await RollbackQuietlyAsync(transaction, cancellationToken).ConfigureAwait(false);
            throw;
        }

        var result = new AppendResult(writtenRows.Positions, null, isDcb ? -1 : writtenRows.LastVersion);
        Signal(result);
        return result;
    }

    private async Task<AppendResult> RecoverSentAppendAsync(
        string streamId,
        IReadOnlyList<Guid> eventIds,
        bool isDcb,
        AppendStatementSentException exception,
        CancellationToken cancellationToken)
    {
        _ = isDcb;
        var landed = await TryReadCommittedAppendAsync(streamId, eventIds, cancellationToken).ConfigureAwait(false);
        if (landed is not null)
        {
            Signal(landed);
            return landed;
        }

        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exception.InnerException ?? exception).Throw();
        throw exception;
    }

    private async Task GrantAppRoleAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var app = new NpgsqlConnectionStringBuilder(_options.ConnectionString).Username;
        var ownerSource = string.IsNullOrWhiteSpace(_options.MigrationConnectionString)
            ? _options.ConnectionString
            : _options.MigrationConnectionString;
        var owner = new NpgsqlConnectionStringBuilder(ownerSource!).Username;
        if (string.IsNullOrWhiteSpace(app) || string.Equals(app, owner, StringComparison.Ordinal))
            return;

        var role = PgIdentifier.Quote(app);
        var schema = PgIdentifier.Quote(_names.Schema);
        var sql = $"""
            GRANT USAGE ON SCHEMA {schema} TO {role};
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {role};
            GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {role};
            GRANT EXECUTE ON ALL ROUTINES IN SCHEMA {schema} TO {role};
            """;
        await ExecuteAsync(connection, transaction, sql, cancellationToken).ConfigureAwait(false);
    }
}
