using System.Data;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using LawnDart.Dcb;
using LawnDart.Snapshots;

namespace LawnDart.EventSourcing.SqlServer.Snapshots;

/// <summary>
/// SQL Server implementation of <see cref="ISnapshotStore"/>, <see cref="IDcbSnapshotStore"/>,
/// and <see cref="ISnapshotAdmin"/>. DCB rows are keyed by the 64-character SHA-256 hex from
/// <see cref="DcbSnapshotId.FromLoadTags"/>; plaintext load tags are stored beside the hash
/// for operations.
/// </summary>
/// <remarks>
/// Latest snapshot per key is upserted (one row). A missing, corrupt, or checksum-mismatched
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
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var schemaSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{SqlServerSnapshotSchema.Quote(_schemaName)}')
            BEGIN
                EXEC('CREATE SCHEMA [{SqlServerSnapshotSchema.Quote(_schemaName)}] AUTHORIZATION dbo');
            END";
        await using (var schemaCmd = new SqlCommand(schemaSql, connection))
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken);

        await SqlServerSnapshotSchema.EnsureCreatedAsync(
            connection, _schemaName, _dcbTableName, _eventTableName, cancellationToken);
    }

    /// <summary>
    /// Loads plaintext load-query tags stored beside the hashed DCB id, or <c>null</c>
    /// when no snapshot exists.
    /// </summary>
    public async Task<IReadOnlyList<string>?> LoadDcbSnapshotTagsAsync(
        string dcbId,
        CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(
            $"SELECT [Tags] FROM {_qDcb} WHERE [DcbId] = @DcbId", connection);
        AddChar64(command, "@DcbId", dcbId);
        var result = await command.ExecuteScalarAsync(ct);
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
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);
            await using var command = new SqlCommand(
                $@"SELECT [GlobalSequence], [TakenAtUtc], [StateData], [Checksum]
                   FROM {_qDcb} WHERE [DcbId] = @DcbId",
                connection);
            AddChar64(command, "@DcbId", dcbId);

            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return (default, null);

            var seq = reader.GetInt64(0);
            var taken = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
            var data = reader.GetString(2);
            var checksum = reader.GetString(3).Trim();

            if (!ChecksumMatches(data, checksum))
            {
                _logger?.LogWarning(
                    "DCB snapshot checksum mismatch for {DcbId}; discarding", dcbId);
                return (default, null);
            }

            var state = JsonSerializer.Deserialize<TState>(data, JsonOptions);
            if (state is null)
                return (default, null);

            return (state, new SnapshotInfo(Version: 0, GlobalSequence: seq, TakenAtUtc: taken));
        }
        catch (JsonException ex)
        {
            _logger?.LogWarning(ex, "DCB snapshot deserialize failed for {DcbId}; discarding", dcbId);
            return (default, null);
        }
        catch (SqlException ex)
        {
            _logger?.LogWarning(ex, "DCB snapshot load failed for {DcbId}; discarding", dcbId);
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

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = new SqlCommand(
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
        await command.ExecuteNonQueryAsync(ct);

        _logger?.LogDebug("Saved DCB snapshot for {DcbId} at global seq {Seq}", dcbId, globalSequence);
    }

    // ── ISnapshotStore ────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task<(TState? State, SnapshotInfo? Info)> LoadSnapshotAsync<TState>(
        string streamId,
        CancellationToken ct = default)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);
            await using var command = new SqlCommand(
                $@"SELECT [Version], [GlobalSequence], [TakenAtUtc], [StateData], [Checksum]
                   FROM {_qEvent} WHERE [StreamId] = @StreamId",
                connection);
            command.Parameters.AddWithValue("@StreamId", streamId);

            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return (default, null);

            var version = reader.GetInt64(0);
            var seq = reader.GetInt64(1);
            var taken = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
            var data = reader.GetString(3);
            var checksum = reader.GetString(4).Trim();

            if (!ChecksumMatches(data, checksum))
            {
                _logger?.LogWarning(
                    "Stream snapshot checksum mismatch for {StreamId}; discarding", streamId);
                return (default, null);
            }

            var state = JsonSerializer.Deserialize<TState>(data, JsonOptions);
            if (state is null)
                return (default, null);

            return (state, new SnapshotInfo(version, seq, taken));
        }
        catch (JsonException ex)
        {
            _logger?.LogWarning(ex, "Stream snapshot deserialize failed for {StreamId}; discarding", streamId);
            return (default, null);
        }
        catch (SqlException ex)
        {
            _logger?.LogWarning(ex, "Stream snapshot load failed for {StreamId}; discarding", streamId);
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
        var data = JsonSerializer.Serialize(state, JsonOptions);
        var checksum = ComputeChecksum(data);
        var taken = DateTime.UtcNow;
        var stateType = typeof(TState).FullName ?? typeof(TState).Name;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(
            $@"
            UPDATE {_qEvent}
            SET [Version] = @Version,
                [GlobalSequence] = @Seq,
                [TakenAtUtc] = @Taken,
                [StateType] = @StateType,
                [StateData] = @StateData,
                [Checksum] = @Checksum
            WHERE [StreamId] = @StreamId;
            IF @@ROWCOUNT = 0
            BEGIN
                INSERT INTO {_qEvent}
                    ([StreamId], [Version], [GlobalSequence], [TakenAtUtc], [StateType], [StateData], [Checksum])
                VALUES
                    (@StreamId, @Version, @Seq, @Taken, @StateType, @StateData, @Checksum);
            END",
            connection);
        command.Parameters.AddWithValue("@StreamId", streamId);
        command.Parameters.AddWithValue("@Version", version);
        command.Parameters.AddWithValue("@Seq", globalSequence);
        command.Parameters.AddWithValue("@Taken", taken);
        command.Parameters.AddWithValue("@StateType", stateType);
        command.Parameters.AddWithValue("@StateData", data);
        AddChar64(command, "@Checksum", checksum);
        await command.ExecuteNonQueryAsync(ct);

        _logger?.LogDebug(
            "Saved snapshot for {StreamId} at version {Version} (global seq {Seq})",
            streamId, version, globalSequence);
    }

    // ── ISnapshotAdmin ────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async Task DeleteSnapshotAsync(string streamId, CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(
            $@"
            DELETE FROM {_qEvent} WHERE [StreamId] = @Id;
            DELETE FROM {_qDcb} WHERE [DcbId] = @Id;",
            connection);
        command.Parameters.AddWithValue("@Id", streamId);
        await command.ExecuteNonQueryAsync(ct);
        _logger?.LogInformation("Deleted snapshots for {StreamId}", streamId);
    }

    /// <inheritdoc/>
    public async Task DeleteAllSnapshotsAsync(CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(
            $"DELETE FROM {_qEvent}; DELETE FROM {_qDcb};", connection);
        await command.ExecuteNonQueryAsync(ct);
        _logger?.LogInformation("Deleted all snapshots from store");
    }

    /// <inheritdoc/>
    public async Task<SnapshotInfo?> GetSnapshotInfoAsync(string streamId, CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(
            $@"
            SELECT [Version], [GlobalSequence], [TakenAtUtc] FROM {_qEvent} WHERE [StreamId] = @Id
            UNION ALL
            SELECT CAST(0 AS BIGINT), [GlobalSequence], [TakenAtUtc] FROM {_qDcb} WHERE [DcbId] = @Id",
            connection);
        command.Parameters.AddWithValue("@Id", streamId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
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
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(
            $@"
            SELECT [StreamId] FROM {_qEvent}
            UNION ALL
            SELECT [DcbId] FROM {_qDcb}",
            connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            yield return reader.GetString(0);
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
