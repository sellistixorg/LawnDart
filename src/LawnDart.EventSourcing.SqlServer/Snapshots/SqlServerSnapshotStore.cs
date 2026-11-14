using System.Data;
using System.Runtime.CompilerServices;
using LawnDart;
using LawnDart.EventStore;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using LawnDart.Dcb;
using LawnDart.Snapshots;
using LawnDart.Sql;

namespace LawnDart.EventSourcing.SqlServer.Snapshots;

/// <summary>
/// SQL Server implementation of <see cref="ISnapshotStore"/>, <see cref="IDcbSnapshotStore"/>,
/// and <see cref="ISnapshotAdmin"/>. DCB rows are keyed by the 64-character SHA-256 hex from
/// <see cref="DcbSnapshotId.FromQuery"/>; a plaintext description of the query is stored beside the hash
/// for operations. Stream rows are keyed by <c>Streams.StreamKey</c>.
/// </summary>
/// <remarks>
/// Latest snapshot per key is upserted (one row). A missing, corrupt, or bad-checksum
/// snapshot returns <c>(default, null)</c> so the repository falls back to full event replay.
/// Register via <c>UseSqlServer(...).WithSnapshots(...)</c>.
/// </remarks>
public sealed class SqlServerSnapshotStore : ISnapshotStore, IDcbSnapshotStore, ISnapshotAdmin
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly string _connectionString;
    private readonly string _schemaName;
    private readonly string _dcbTableName;
    private readonly string _eventTableName;
    private readonly string _qDcb;
    private readonly string _qEvent;
    private readonly string _qRegistry;
    private readonly string _qStreamKeySequence;
    private readonly ILogger<SqlServerSnapshotStore>? _logger;

    /// <summary>
    /// Initializes a SQL snapshot store using <paramref name="options"/> for connection,
    /// schema, and table names.
    /// </summary>
    public SqlServerSnapshotStore(
        SqlServerEventStoreOptions options,
        string? eventsTableName = null,
        ILogger<SqlServerSnapshotStore>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw new ArgumentException("ConnectionString must be set.", nameof(options));

        _connectionString = options.ConnectionString;
        _schemaName = string.IsNullOrWhiteSpace(options.SchemaName) ? "dbo" : options.SchemaName;
        var eventsTable = eventsTableName
            ?? (string.IsNullOrWhiteSpace(options.EventStoreTableName) ? "Events" : options.EventStoreTableName);
        _dcbTableName = SqlServerSnapshotSchema.ResolveTableName(
            options.DcbSnapshotsTableName, SqlServerSnapshotSchema.DefaultDcbTable, eventsTable);
        _eventTableName = SqlServerSnapshotSchema.ResolveTableName(
            options.EventSnapshotsTableName, SqlServerSnapshotSchema.DefaultEventTable, eventsTable);
        _qDcb = SqlServerSnapshotSchema.Qualify(_schemaName, _dcbTableName);
        _qEvent = SqlServerSnapshotSchema.Qualify(_schemaName, _eventTableName);
        var registry = string.IsNullOrWhiteSpace(options.StreamRegistryTableName)
            ? "Streams"
            : options.StreamRegistryTableName;
        _qRegistry = SqlServerSnapshotSchema.Qualify(_schemaName, registry);
        _qStreamKeySequence = SqlIdentifier.Qualify(_schemaName, "StreamKeySequence");
        _logger = logger;
    }

    /// <summary>Schema-qualified DCB snapshot table name.</summary>
    public string QualifiedDcbTable => _qDcb;

    /// <summary>Schema-qualified stream snapshot table name.</summary>
    public string QualifiedEventTable => _qEvent;

    /// <summary>
    /// Creates <c>DcbSnapshots</c> and <c>EventSnapshots</c> if they do not exist.
    /// Idempotent. The event store's <c>InitializeSchemaAsync</c> also creates these tables.
    /// </summary>
    /// <remarks>
    /// Run this from a deploy step. Normal app startup should not call it.
    /// </remarks>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var schemaSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{SqlIdentifier.Literal(_schemaName)}')
            BEGIN
                EXEC('CREATE SCHEMA {SqlIdentifier.Literal(SqlIdentifier.Quote(_schemaName))} AUTHORIZATION dbo');
            END";
        var schemaCmd = new SqlCommand(schemaSql, connection);
        await using (schemaCmd.ConfigureAwait(false))
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await SqlServerSnapshotSchema.EnsureCreatedAsync(
            connection, _schemaName, _dcbTableName, _eventTableName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads whether <c>EventSnapshots</c> and <c>DcbSnapshots</c> are present.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A match when both tables exist. This store has no format stamp.</returns>
    /// <remarks>
    /// This method does not create or change objects. Call it at startup.
    /// <see cref="SchemaCheckResult.ThrowIfMismatch"/> throws when the result does not match.
    /// </remarks>
    public async Task<SchemaCheckResult> VerifySchemaAsync(CancellationToken cancellationToken = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var disposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await SqlSchemaProbe.RequireViewDefinitionAsync(connection, _schemaName, cancellationToken).ConfigureAwait(false);

        var missing = new List<string>();
        await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, _eventTableName, "U", missing, cancellationToken).ConfigureAwait(false);
        await SqlSchemaProbe.AddMissingAsync(connection, _schemaName, _dcbTableName, "U", missing, cancellationToken).ConfigureAwait(false);
        return SqlSchemaProbe.Finish(expected: null, found: null, missing);
    }

    /// <summary>
    /// Loads plaintext load-query tags stored beside the hashed DCB id, or <c>null</c>
    /// when no snapshot exists.
    /// </summary>
    public async Task<IReadOnlyList<string>?> LoadDcbSnapshotTagsAsync(
        string dcbId,
        CancellationToken ct = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        var command = new SqlCommand(
            $"SELECT [Tags] FROM {_qDcb} WHERE [DcbId] = @DcbId", connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        AddChar64(command, "@DcbId", dcbId);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        if (result is null or DBNull)
            return null;
        return JsonSerializer.Deserialize<string[]>((string)result, JsonOptions);
    }

    // ── IDcbSnapshotStore ─────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<(TState? State, SnapshotInfo? Info)> LoadDcbSnapshotAsync<TState>(
        string dcbId,
        CancellationToken ct = default)
    {
        try
        {
            var connection = new SqlConnection(_connectionString);
            await using var connectionDisposal = connection.ConfigureAwait(false);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            var command = new SqlCommand(
                $@"SELECT [GlobalSequence], [TakenAtUtc], [StateData], [Checksum]
                   FROM {_qDcb} WHERE [DcbId] = @DcbId",
                connection);
            await using var commandDisposal = command.ConfigureAwait(false);
            AddChar64(command, "@DcbId", dcbId);

            var reader = (await command.ExecuteReaderAsync(ct).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return (default, null);

            var seq = reader.GetInt64(0);
            var taken = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
            var data = reader.GetString(2);
            var checksum = reader.GetString(3).Trim();

            if (!ChecksumMatches(data, checksum))
            {
                if (_logger is not null)
                    SqlServerSnapshotStoreLog.DcbChecksumMismatch(_logger, dcbId);
                return (default, null);
            }

            var state = JsonSerializer.Deserialize<TState>(data, JsonOptions);
            if (state is null)
                return (default, null);

            return (state, new SnapshotInfo(Version: 0, GlobalSequence: seq, TakenAtUtc: taken));
        }
        catch (JsonException ex)
        {
            if (_logger is not null)
                SqlServerSnapshotStoreLog.DcbDeserializeFailed(_logger, ex, dcbId);
            return (default, null);
        }
        catch (SqlException ex)
        {
            if (_logger is not null)
                SqlServerSnapshotStoreLog.DcbLoadFailed(_logger, ex, dcbId);
            return (default, null);
        }
    }

    /// <inheritdoc/>
    public async Task SaveDcbSnapshotAsync<TState>(
        string dcbId,
        long globalSequence,
        TState state,
        byte[]? consistencyMarker = null,
        IReadOnlyList<string>? loadTags = null,
        CancellationToken ct = default)
    {
        var data = JsonSerializer.Serialize(state, JsonOptions);
        var checksum = ComputeChecksum(data);
        var taken = DateTime.UtcNow;
        var stateType = typeof(TState).FullName ?? typeof(TState).Name;

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(ct).ConfigureAwait(false);

        var command = new SqlCommand(
            $@"
            UPDATE {_qDcb}
            SET [GlobalSequence] = @Seq,
                [TakenAtUtc] = @Taken,
                [StateType] = @StateType,
                [StateData] = @StateData,
                [Tags] = @Tags,
                [ConsistencyMarker] = @Marker,
                [Checksum] = @Checksum
            WHERE [DcbId] = @DcbId;
            IF @@ROWCOUNT = 0
            BEGIN
                INSERT INTO {_qDcb}
                    ([DcbId], [GlobalSequence], [TakenAtUtc], [StateType], [StateData], [Tags], [ConsistencyMarker], [Checksum])
                VALUES
                    (@DcbId, @Seq, @Taken, @StateType, @StateData, @Tags, @Marker, @Checksum);
            END",
            connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        AddChar64(command, "@DcbId", dcbId);
        command.Parameters.AddWithValue("@Seq", globalSequence);
        command.Parameters.AddWithValue("@Taken", taken);
        command.Parameters.AddWithValue("@StateType", stateType);
        command.Parameters.AddWithValue("@StateData", data);
        var tagsParam = command.Parameters.Add("@Tags", SqlDbType.NVarChar, -1);
        tagsParam.Value = loadTags is null ? DBNull.Value : JsonSerializer.Serialize(loadTags, JsonOptions);
        var markerParam = command.Parameters.Add("@Marker", SqlDbType.VarBinary, 512);
        markerParam.Value = consistencyMarker is null ? DBNull.Value : consistencyMarker;
        AddChar64(command, "@Checksum", checksum);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        if (_logger is not null)
            SqlServerSnapshotStoreLog.DcbSaved(_logger, dcbId, globalSequence);
    }

    // ── ISnapshotStore ────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<(TState? State, SnapshotInfo? Info)> LoadSnapshotAsync<TState>(
        string streamId,
        CancellationToken ct = default)
    {
        try
        {
            var connection = new SqlConnection(_connectionString);
            await using var connectionDisposal = connection.ConfigureAwait(false);
            await connection.OpenAsync(ct).ConfigureAwait(false);
            var command = new SqlCommand(
                $@"SELECT e.[Version], e.[GlobalSequence], e.[TakenAtUtc], e.[StateData], e.[Checksum]
                   FROM {_qEvent} e
                   INNER JOIN {_qRegistry} s ON s.[StreamKey] = e.[StreamKey]
                   WHERE s.[StreamId] = @StreamId",
                connection);
            await using var commandDisposal = command.ConfigureAwait(false);
            command.Parameters.AddWithValue("@StreamId", streamId);

            var reader = (await command.ExecuteReaderAsync(ct).ConfigureAwait(false));
            await using var readerDisposal = reader.ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return (default, null);

            var version = reader.GetInt64(0);
            var seq = reader.GetInt64(1);
            var taken = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
            var data = reader.GetString(3);
            var checksum = reader.GetString(4).Trim();

            if (!ChecksumMatches(data, checksum))
            {
                if (_logger is not null)
                    SqlServerSnapshotStoreLog.StreamChecksumMismatch(_logger, streamId);
                return (default, null);
            }

            var state = JsonSerializer.Deserialize<TState>(data, JsonOptions);
            if (state is null)
                return (default, null);

            return (state, new SnapshotInfo(version, seq, taken));
        }
        catch (JsonException ex)
        {
            if (_logger is not null)
                SqlServerSnapshotStoreLog.StreamDeserializeFailed(_logger, ex, streamId);
            return (default, null);
        }
        catch (SqlException ex)
        {
            if (_logger is not null)
                SqlServerSnapshotStoreLog.StreamLoadFailed(_logger, ex, streamId);
            return (default, null);
        }
    }

    /// <inheritdoc/>
    public async Task SaveSnapshotAsync<TState>(
        string streamId,
        long version,
        long globalSequence,
        TState state,
        CancellationToken ct = default)
    {
        SqlServerColumnLimits.EnsureStreamId(streamId, nameof(streamId));

        var data = JsonSerializer.Serialize(state, JsonOptions);
        var checksum = ComputeChecksum(data);
        var taken = DateTime.UtcNow;
        var stateType = typeof(TState).FullName ?? typeof(TState).Name;

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        var streamKey = await EnsureStreamKeyAsync(connection, streamId, ct).ConfigureAwait(false);
        var command = new SqlCommand(
            $@"
            UPDATE {_qEvent}
            SET [Version] = @Version,
                [GlobalSequence] = @Seq,
                [TakenAtUtc] = @Taken,
                [StateType] = @StateType,
                [StateData] = @StateData,
                [Checksum] = @Checksum
            WHERE [StreamKey] = @StreamKey;
            IF @@ROWCOUNT = 0
            BEGIN
                INSERT INTO {_qEvent}
                    ([StreamKey], [Version], [GlobalSequence], [TakenAtUtc], [StateType], [StateData], [Checksum])
                VALUES
                    (@StreamKey, @Version, @Seq, @Taken, @StateType, @StateData, @Checksum);
            END",
            connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.Add(new SqlParameter("@StreamKey", streamKey));
        command.Parameters.AddWithValue("@Version", version);
        command.Parameters.AddWithValue("@Seq", globalSequence);
        command.Parameters.AddWithValue("@Taken", taken);
        command.Parameters.AddWithValue("@StateType", stateType);
        command.Parameters.AddWithValue("@StateData", data);
        AddChar64(command, "@Checksum", checksum);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        if (_logger is not null)
            SqlServerSnapshotStoreLog.StreamSaved(_logger, streamId, version, globalSequence);
    }

    // ── ISnapshotAdmin ────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task DeleteSnapshotAsync(string streamId, CancellationToken ct = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        var command = new SqlCommand(
            $@"
            DELETE e
            FROM {_qEvent} e
            INNER JOIN {_qRegistry} s ON s.[StreamKey] = e.[StreamKey]
            WHERE s.[StreamId] = @Id;
            DELETE FROM {_qDcb} WHERE [DcbId] = @Id;",
            connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@Id", streamId);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        _logger?.LogInformation("Deleted snapshots for {StreamId}", streamId);
    }

    /// <inheritdoc/>
    public async Task DeleteAllSnapshotsAsync(CancellationToken ct = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        var command = new SqlCommand(
            $"DELETE FROM {_qEvent}; DELETE FROM {_qDcb};", connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        _logger?.LogInformation("Deleted all snapshots from store");
    }

    /// <inheritdoc/>
    public async Task<SnapshotInfo?> GetSnapshotInfoAsync(string streamId, CancellationToken ct = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        var command = new SqlCommand(
            $@"
            SELECT e.[Version], e.[GlobalSequence], e.[TakenAtUtc]
            FROM {_qEvent} e
            INNER JOIN {_qRegistry} s ON s.[StreamKey] = e.[StreamKey]
            WHERE s.[StreamId] = @Id
            UNION ALL
            SELECT CAST(0 AS BIGINT), [GlobalSequence], [TakenAtUtc] FROM {_qDcb} WHERE [DcbId] = @Id",
            connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@Id", streamId);
        var reader = (await command.ExecuteReaderAsync(ct).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;
        return new SnapshotInfo(
            reader.GetInt64(0),
            reader.GetInt64(1),
            DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc));
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<string> EnumerateSnapshotStreamsAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(ct).ConfigureAwait(false);
        var command = new SqlCommand(
            $@"
            SELECT s.[StreamId]
            FROM {_qEvent} e
            INNER JOIN {_qRegistry} s ON s.[StreamKey] = e.[StreamKey]
            UNION ALL
            SELECT [DcbId] FROM {_qDcb}",
            connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        var reader = (await command.ExecuteReaderAsync(ct).ConfigureAwait(false));
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            yield return reader.GetString(0);
    }

    private async Task<long> EnsureStreamKeyAsync(
        SqlConnection connection,
        string streamId,
        CancellationToken cancellationToken)
    {
        var transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using var transactionDisposal = transaction.ConfigureAwait(false);
        var key = await ReadStreamKeyAsync(connection, transaction, streamId, lockRow: true, cancellationToken).ConfigureAwait(false);
        if (key is null)
        {
            var tenantId = StreamIdParser.ExtractTenantId(streamId);
            var aggregateType = StreamIdParser.ExtractAggregateType(streamId);
            var aggregateId = StreamIdParser.ExtractAggregateId(streamId);
            var now = DateTime.UtcNow;
            var insert = new SqlCommand(
                $"""
                DECLARE @keys TABLE (StreamKey BIGINT NOT NULL);
                INSERT INTO {_qRegistry}
                (StreamKey, StreamId, TenantId, AggregateType, AggregateId, CurrentVersion, LastSequencePosition,
                 CreatedAt, LastEventAt, EventCount, Tags, Status)
                OUTPUT INSERTED.StreamKey INTO @keys
                VALUES
                (NEXT VALUE FOR {_qStreamKeySequence}, @StreamId, @TenantId, @AggregateType, @AggregateId, -1, 0,
                 @CreatedAt, @LastEventAt, 0, N'[]', N'Active');
                SELECT StreamKey FROM @keys;
                """,
                connection,
                transaction);
            await using var insertDisposal = insert.ConfigureAwait(false);
            insert.Parameters.Add(new SqlParameter("@StreamId", streamId));
            insert.Parameters.Add(new SqlParameter("@TenantId", SqlDbType.NVarChar, 100) { Value = (object?)tenantId ?? DBNull.Value });
            insert.Parameters.Add(new SqlParameter("@AggregateType", aggregateType));
            insert.Parameters.Add(new SqlParameter("@AggregateId", SqlDbType.UniqueIdentifier) { Value = aggregateId.HasValue ? aggregateId.Value : DBNull.Value });
            insert.Parameters.Add(new SqlParameter("@CreatedAt", now));
            insert.Parameters.Add(new SqlParameter("@LastEventAt", now));
            try
            {
                key = Convert.ToInt64(await insert.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
            }
            catch (SqlException ex) when (ex.Number is 2627 or 2601)
            {
                key = await ReadStreamKeyAsync(connection, transaction, streamId, lockRow: false, cancellationToken).ConfigureAwait(false);
            }
        }

        if (key is null)
            throw new InvalidOperationException($"Stream '{streamId}' is not in {_qRegistry}.");

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return key.Value;
    }

    private async Task<long?> ReadStreamKeyAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        bool lockRow,
        CancellationToken cancellationToken)
    {
        var hint = lockRow ? " WITH (UPDLOCK, ROWLOCK)" : "";
        var command = new SqlCommand(
            $"SELECT StreamKey FROM {_qRegistry}{hint} WHERE StreamId = @StreamId",
            connection,
            transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.Add(new SqlParameter("@StreamId", streamId));
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull ? null : Convert.ToInt64(result);
    }

    private static void AddChar64(SqlCommand command, string name, string value)
    {
        var p = command.Parameters.Add(name, SqlDbType.Char, 64);
        p.Value = value;
    }

    private static string ComputeChecksum(string stateData)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(stateData));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private static bool ChecksumMatches(string stateData, string stored)
        => string.Equals(ComputeChecksum(stateData), stored, StringComparison.Ordinal);
}
