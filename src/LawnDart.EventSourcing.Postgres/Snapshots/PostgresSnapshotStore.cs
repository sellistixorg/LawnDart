using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.Snapshots;

namespace LawnDart.EventSourcing.Postgres.Snapshots;

/// <summary>
/// Postgres implementation of <see cref="ISnapshotStore"/>, <see cref="IDcbSnapshotStore"/>,
/// and <see cref="ISnapshotAdmin"/>. Dynamic consistency boundary rows are keyed by the
/// 64-character SHA-256 hex from <see cref="DcbSnapshotId.FromLoadTags"/>. Plaintext load
/// tags are stored beside the hash.
/// </summary>
/// <remarks>
/// One row per key. A write replaces that row. State is stored as bytes so the checksum
/// matches those bytes. A missing snapshot, a bad checksum, or a snapshot that cannot be
/// read returns <c>(default, null)</c> and the repository replays the events.
/// Register via <c>UsePostgres(...).WithSnapshots(...)</c>.
/// </remarks>
public sealed class PostgresSnapshotStore : ISnapshotStore, IDcbSnapshotStore, ISnapshotAdmin
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly PostgresEventStoreOptions _options;
    private readonly string _events;
    private readonly string _dcb;
    private readonly ILogger<PostgresSnapshotStore>? _logger;

    /// <summary>Creates a snapshot store from <paramref name="options"/>.</summary>
    public PostgresSnapshotStore(PostgresEventStoreOptions options, ILogger<PostgresSnapshotStore>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw new ArgumentException("ConnectionString must be set.", nameof(options));
        PgIdentifier.RequireSchema(options.SchemaName);
        _options = options;
        var schema = options.SchemaName;
        _events = PgIdentifier.Qualify(schema, PostgresSnapshotSchema.EventTable(options));
        _dcb = PgIdentifier.Qualify(schema, PostgresSnapshotSchema.DcbTable(options));
        _logger = logger;
    }

    /// <summary>Schema-qualified stream snapshot table.</summary>
    public string QualifiedEventTable => _events;

    /// <summary>Schema-qualified dynamic consistency boundary snapshot table.</summary>
    public string QualifiedDcbTable => _dcb;

    /// <summary>
    /// Creates <c>event_snapshots</c> and <c>dcb_snapshots</c> when they are missing.
    /// <c>InitializeSchemaAsync</c> on the event store also creates these tables.
    /// </summary>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        var migration = string.IsNullOrWhiteSpace(_options.MigrationConnectionString)
            ? _options.ConnectionString!
            : _options.MigrationConnectionString;
        await using var connection = await PostgresEventStore.Source(migration).OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await PostgresSnapshotSchema.EnsureAsync(connection, transaction: null, _options, cancellationToken).ConfigureAwait(false);
        await GrantAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads the plaintext load-query tags stored beside the hashed id, or null when no snapshot exists.
    /// </summary>
    public async Task<IReadOnlyList<string>?> LoadDcbSnapshotTagsAsync(string dcbId, CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand($"SELECT tags FROM {_dcb} WHERE dcb_id = @id", connection);
        command.Parameters.AddWithValue("id", dcbId);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        if (result is null or DBNull)
            return null;
        return (string[])result;
    }

    /// <inheritdoc/>
    public async Task<(TState? State, SnapshotInfo? Info)> LoadDcbSnapshotAsync<TState>(
        string dcbId, CancellationToken ct = default)
    {
        try
        {
            await using var connection = await OpenAsync(ct).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(
                $"""
                SELECT global_position, taken_at, state_data, checksum
                FROM {_dcb} WHERE dcb_id = @id
                """,
                connection);
            command.Parameters.AddWithValue("id", dcbId);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return (default, null);

            var seq = reader.GetInt64(0);
            var taken = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
            var data = (byte[])reader.GetValue(2);
            var checksum = reader.GetString(3).Trim();
            if (!ChecksumMatches(data, checksum))
            {
                if (_logger is not null)
                    PostgresSnapshotStoreLog.DcbChecksumMismatch(_logger, dcbId);
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
                PostgresSnapshotStoreLog.DcbDeserializeFailed(_logger, ex, dcbId);
            return (default, null);
        }
        catch (NpgsqlException ex)
        {
            if (_logger is not null)
                PostgresSnapshotStoreLog.DcbLoadFailed(_logger, ex, dcbId);
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
        var data = JsonSerializer.SerializeToUtf8Bytes(state, JsonOptions);
        var checksum = ComputeChecksum(data);
        var stateType = typeof(TState).FullName ?? typeof(TState).Name;
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
            INSERT INTO {_dcb} AS s
                (dcb_id, global_position, taken_at, state_type, state_data, tags, consistency_marker, checksum)
            VALUES (@id, @seq, now(), @state_type, @data, @tags, @marker, @checksum)
            ON CONFLICT (dcb_id) DO UPDATE
            SET global_position = EXCLUDED.global_position,
                taken_at = EXCLUDED.taken_at,
                state_type = EXCLUDED.state_type,
                state_data = EXCLUDED.state_data,
                tags = EXCLUDED.tags,
                consistency_marker = EXCLUDED.consistency_marker,
                checksum = EXCLUDED.checksum
            """,
            connection);
        command.Parameters.AddWithValue("id", dcbId);
        command.Parameters.AddWithValue("seq", globalSequence);
        command.Parameters.AddWithValue("state_type", stateType);
        command.Parameters.Add(new NpgsqlParameter("data", NpgsqlDbType.Bytea) { Value = data });
        command.Parameters.Add(new NpgsqlParameter("tags", NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            Value = loadTags is null ? DBNull.Value : loadTags.ToArray()
        });
        command.Parameters.Add(new NpgsqlParameter("marker", NpgsqlDbType.Bytea)
        {
            Value = consistencyMarker is null ? DBNull.Value : consistencyMarker
        });
        command.Parameters.AddWithValue("checksum", checksum);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (_logger is not null)
            PostgresSnapshotStoreLog.DcbSaved(_logger, dcbId, globalSequence);
    }

    /// <inheritdoc/>
    public async Task<(TState? State, SnapshotInfo? Info)> LoadSnapshotAsync<TState>(
        string streamId, CancellationToken ct = default)
    {
        try
        {
            var tenant = TenantKey.ForStream(streamId);
            await using var connection = await OpenAsync(ct).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(
                $"""
                SELECT version, global_position, taken_at, state_data, checksum
                FROM {_events} WHERE tenant_id = @tenant AND stream_id = @stream
                """,
                connection);
            command.Parameters.AddWithValue("tenant", tenant);
            command.Parameters.AddWithValue("stream", streamId);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return (default, null);

            var version = reader.GetInt64(0);
            var seq = reader.GetInt64(1);
            var taken = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
            var data = (byte[])reader.GetValue(3);
            var checksum = reader.GetString(4).Trim();
            if (!ChecksumMatches(data, checksum))
            {
                if (_logger is not null)
                    PostgresSnapshotStoreLog.StreamChecksumMismatch(_logger, streamId);
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
                PostgresSnapshotStoreLog.StreamDeserializeFailed(_logger, ex, streamId);
            return (default, null);
        }
        catch (NpgsqlException ex)
        {
            if (_logger is not null)
                PostgresSnapshotStoreLog.StreamLoadFailed(_logger, ex, streamId);
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
        if (streamId.Length > PostgresEventStore.StreamIdMaxLength)
        {
            throw new ArgumentException(
                $"Stream id is {streamId.Length} characters. The store keeps stream ids to {PostgresEventStore.StreamIdMaxLength} characters so a stream can move between backends. Shorten the id.",
                nameof(streamId));
        }

        var tenant = TenantKey.ForStream(streamId);
        var data = JsonSerializer.SerializeToUtf8Bytes(state, JsonOptions);
        var checksum = ComputeChecksum(data);
        var stateType = typeof(TState).FullName ?? typeof(TState).Name;
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
            INSERT INTO {_events}
                (tenant_id, stream_id, version, global_position, taken_at, state_type, state_data, checksum)
            VALUES (@tenant, @stream, @version, @seq, now(), @state_type, @data, @checksum)
            ON CONFLICT (tenant_id, stream_id) DO UPDATE
            SET version = EXCLUDED.version,
                global_position = EXCLUDED.global_position,
                taken_at = EXCLUDED.taken_at,
                state_type = EXCLUDED.state_type,
                state_data = EXCLUDED.state_data,
                checksum = EXCLUDED.checksum
            """,
            connection);
        command.Parameters.AddWithValue("tenant", tenant);
        command.Parameters.AddWithValue("stream", streamId);
        command.Parameters.AddWithValue("version", version);
        command.Parameters.AddWithValue("seq", globalSequence);
        command.Parameters.AddWithValue("state_type", stateType);
        command.Parameters.Add(new NpgsqlParameter("data", NpgsqlDbType.Bytea) { Value = data });
        command.Parameters.AddWithValue("checksum", checksum);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (_logger is not null)
            PostgresSnapshotStoreLog.StreamSaved(_logger, streamId, version, globalSequence);
    }

    /// <inheritdoc/>
    public async Task DeleteSnapshotAsync(string streamId, CancellationToken ct = default)
    {
        var tenant = TenantKey.ForStream(streamId);
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
            DELETE FROM {_events} WHERE tenant_id = @tenant AND stream_id = @id;
            DELETE FROM {_dcb} WHERE dcb_id = @id;
            """,
            connection);
        command.Parameters.AddWithValue("tenant", tenant);
        command.Parameters.AddWithValue("id", streamId);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task DeleteAllSnapshotsAsync(CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand($"DELETE FROM {_events}; DELETE FROM {_dcb};", connection);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<SnapshotInfo?> GetSnapshotInfoAsync(string streamId, CancellationToken ct = default)
    {
        var tenant = TenantKey.ForStream(streamId);
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
            SELECT version, global_position, taken_at FROM {_events}
            WHERE tenant_id = @tenant AND stream_id = @id
            UNION ALL
            SELECT 0::bigint, global_position, taken_at FROM {_dcb} WHERE dcb_id = @id
            """,
            connection);
        command.Parameters.AddWithValue("tenant", tenant);
        command.Parameters.AddWithValue("id", streamId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
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
        await using var connection = await OpenAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
            SELECT stream_id FROM {_events}
            UNION ALL
            SELECT dcb_id FROM {_dcb}
            """,
            connection);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            yield return reader.GetString(0).Trim();
    }

    private async Task GrantAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var app = new NpgsqlConnectionStringBuilder(_options.ConnectionString).Username;
        var ownerSource = string.IsNullOrWhiteSpace(_options.MigrationConnectionString)
            ? _options.ConnectionString!
            : _options.MigrationConnectionString;
        var owner = new NpgsqlConnectionStringBuilder(ownerSource).Username;
        if (string.IsNullOrWhiteSpace(app) || string.Equals(app, owner, StringComparison.Ordinal))
            return;

        var schema = PgIdentifier.Quote(PgIdentifier.RequireSchema(_options.SchemaName));
        var role = PgIdentifier.Quote(app);
        await using var command = new NpgsqlCommand(
            $"""
            GRANT USAGE ON SCHEMA {schema} TO {role};
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {role};
            """,
            connection);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private ValueTask<NpgsqlConnection> OpenAsync(CancellationToken cancellationToken)
        => PostgresEventStore.Source(_options.ConnectionString!).OpenConnectionAsync(cancellationToken);

    private static string ComputeChecksum(byte[] data)
        => Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

    private static bool ChecksumMatches(byte[] data, string stored)
        => string.Equals(ComputeChecksum(data), stored, StringComparison.Ordinal);
}
