using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// Caught-up sequence for one store instance.
/// </summary>
/// <remarks>
/// <para>
/// A Postgres sequence hands out a number before the writer commits, and a
/// crash can skip numbers. A reader that walks past a hole can miss an event
/// that commits later. This type keeps a mark M. Readers treat every position
/// at or below M as settled.
/// </para>
/// <para>
/// Proof. Every append calls <c>pg_current_xact_id()</c> before <c>nextval</c>,
/// so a writer that took position g already has a transaction id. A reader
/// loads committed positions and <c>pg_current_snapshot()</c> in one statement.
/// Suppose position p is visible, p &gt; g, and g is missing. The writer of g
/// took its id before <c>nextval(g)</c>, which is before <c>nextval(p)</c>,
/// which is before p committed (p is visible). That writer is therefore either
/// already finished at this snapshot, or still in progress and listed in
/// <c>pg_snapshot_xip</c>. When every id on that list has finished
/// (<c>pg_xact_status</c> is no longer <c>in progress</c>), a reread of the
/// hole either finds the row or the number was never committed. M then moves
/// past it. A transaction that starts after the snapshot cannot hold g,
/// because g is below a position that is already committed.
/// </para>
/// <para>
/// An unrelated write is on that list too. It holds M only while a hole is
/// open. After <see cref="PostgresEventStoreOptions.ReadHorizonGracePeriod"/>
/// the reader moves to the highest committed position, logs a warning, and
/// watches the hole. A row that shows up in the hole later is logged as an error.
/// A read-only transaction has no transaction id and does not hold M.
/// </para>
/// </remarks>
internal sealed class PostgresReadHorizon
{
    private readonly PostgresNames _names;
    private readonly PostgresEventStoreOptions _options;
    private readonly ILogger? _logger;
    private readonly object _sync = new();
    private readonly List<WatchedRange> _watched = [];
    private readonly HashSet<long> _reported = [];
    private long _mark;
    private string? _holdKey;
    private DateTime _holdSinceUtc;
    private bool _warned;

    internal PostgresReadHorizon(PostgresNames names, PostgresEventStoreOptions options, ILogger? logger)
    {
        _names = names;
        _options = options;
        _logger = logger;
    }

    internal async Task<long> ResolveAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        for (var pass = 0; pass < 16; pass++)
        {
            var sample = await ReadSampleAsync(connection, cancellationToken).ConfigureAwait(false);
            await ObserveLateAsync(connection, cancellationToken).ConfigureAwait(false);

            if (sample.NextAfter is null)
            {
                MoveMark(sample.Contiguous);
                ClearHold();
                return Mark;
            }

            var open = await StillOpenAsync(connection, sample.InProgress, cancellationToken).ConfigureAwait(false);
            if (open.Count == 0)
            {
                // Every writer that could fill the hole has finished.
                // A reread treats anything still missing as a dead number.
                var present = await PositionsBetweenAsync(
                    connection, sample.Contiguous, sample.NextAfter.Value, cancellationToken).ConfigureAwait(false);
                var walked = Walk(sample.Contiguous, present);
                if (walked + 1 < sample.NextAfter.Value)
                    walked = sample.NextAfter.Value - 1;
                MoveMark(walked);
                ClearHold();
                continue;
            }

            MoveMark(sample.Contiguous);
            var held = await HoldMillisecondsAsync(connection, open, cancellationToken).ConfigureAwait(false);
            if (WaitsForever(_options.ReadHorizonGracePeriod) || held <= _options.ReadHorizonGracePeriod.TotalMilliseconds)
                return Mark;

            Remember(sample.Contiguous, sample.Committed, await PositionsBetweenAsync(
                connection, sample.Contiguous, sample.Committed + 1, cancellationToken).ConfigureAwait(false));
            Warn(held, open, sample.Contiguous, sample.Committed);
            MoveMark(sample.Committed);
            return Mark;
        }

        return Mark;
    }

    private long Mark
    {
        get { lock (_sync) return _mark; }
    }

    private void MoveMark(long value)
    {
        lock (_sync)
        {
            if (value > _mark)
                _mark = value;
        }
    }

    private void ClearHold()
    {
        lock (_sync)
        {
            _holdKey = null;
            _warned = false;
        }
    }

    private async Task<Sample> ReadSampleAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        long mark;
        lock (_sync)
            mark = _mark;

        var sql = $"""
            WITH nxt AS (
                SELECT e.global_position
                FROM {_names.Events} e
                WHERE e.global_position > @mark + 1
                  AND NOT EXISTS (
                      SELECT 1 FROM {_names.Events} p WHERE p.global_position = e.global_position - 1)
                ORDER BY e.global_position
                LIMIT 1
            )
            SELECT
                pg_current_snapshot()::text,
                COALESCE(
                    (SELECT MAX(e.global_position)
                     FROM {_names.Events} e
                     WHERE e.global_position > @mark
                       AND (
                           NOT EXISTS (SELECT 1 FROM nxt)
                           OR e.global_position < (SELECT global_position FROM nxt)
                       )),
                    @mark) AS contiguous,
                (SELECT global_position FROM nxt) AS next_after,
                COALESCE((SELECT MAX(global_position) FROM {_names.Events}), 0) AS committed
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("mark", NpgsqlDbType.Bigint) { Value = mark });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return new Sample(mark, null, 0, []);

        var snapshot = reader.GetString(0);
        var contiguous = reader.GetInt64(1);
        long? next = reader.IsDBNull(2) ? null : reader.GetInt64(2);
        var committed = reader.GetInt64(3);
        return new Sample(contiguous, next, committed, ParseXip(snapshot));
    }

    private static List<string> ParseXip(string snapshot)
    {
        var parts = snapshot.Split(':');
        if (parts.Length < 3 || parts[2].Length == 0)
            return [];
        return parts[2].Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();
    }

    private async Task<List<string>> StillOpenAsync(
        NpgsqlConnection connection,
        IReadOnlyList<string> xids,
        CancellationToken cancellationToken)
    {
        if (xids.Count == 0)
            return [];

        const string sql = """
            SELECT id, pg_xact_status(id::xid8)
            FROM unnest(@ids::text[]) AS id
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = xids.ToArray() });
        var open = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!reader.IsDBNull(1) && reader.GetString(1) == "in progress")
                open.Add(reader.GetString(0));
        }

        return open;
    }

    private async Task<long> HoldMillisecondsAsync(
        NpgsqlConnection connection,
        IReadOnlyList<string> open,
        CancellationToken cancellationToken)
    {
        var key = string.Join(',', open.OrderBy(static id => id, StringComparer.Ordinal));
        long fromDatabase = 0;
        string? session = null;
        string? transaction = null;
        var sql = """
            SELECT pid::text,
                   backend_xid::text,
                   COALESCE((EXTRACT(EPOCH FROM (clock_timestamp() - xact_start)) * 1000)::bigint, 0)
            FROM pg_stat_activity
            WHERE backend_xid IS NOT NULL
              AND backend_xid::text = ANY(@ids::text[])
            ORDER BY xact_start
            LIMIT 1
            """;
        await using (var command = new NpgsqlCommand(sql, connection))
        {
            command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = open.ToArray() });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                session = reader.GetString(0);
                transaction = reader.IsDBNull(1) ? open[0] : reader.GetString(1);
                fromDatabase = reader.GetInt64(2);
            }
        }

        lock (_sync)
        {
            if (_holdKey != key)
            {
                _holdKey = key;
                _holdSinceUtc = DateTime.UtcNow;
                _warned = false;
                _sessionId = session ?? "0";
                _transactionId = transaction ?? open[0];
            }
            else if (session is not null)
            {
                _sessionId = session;
                _transactionId = transaction ?? _transactionId;
            }

            var local = (long)(DateTime.UtcNow - _holdSinceUtc).TotalMilliseconds;
            return Math.Max(fromDatabase, local < 0 ? 0 : local);
        }
    }

    private string _sessionId = "0";
    private string _transactionId = "0";

    private void Warn(long heldMs, IReadOnlyList<string> open, long stable, long committed)
    {
        string session;
        string transaction;
        var should = false;
        lock (_sync)
        {
            if (!_warned && _logger is not null)
            {
                _warned = true;
                should = true;
            }

            session = _sessionId;
            transaction = _transactionId.Length == 0 ? open[0] : _transactionId;
        }

        if (should)
        {
            PostgresEventStoreLog.ReadHorizonAdvanced(
                _logger!, heldMs, session, transaction, stable, committed);
        }
    }

    private void Remember(long after, long before, IReadOnlyList<long> seen)
    {
        if (before <= after)
            return;
        lock (_sync)
        {
            _watched.Add(new WatchedRange(after, before, seen.ToHashSet()));
        }
    }

    private async Task ObserveLateAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        WatchedRange[] ranges;
        lock (_sync)
            ranges = _watched.ToArray();
        if (ranges.Length == 0 || _logger is null)
            return;

        foreach (var range in ranges)
        {
            var rows = await RowsBetweenAsync(connection, range.After, range.Before, cancellationToken).ConfigureAwait(false);
            foreach (var (position, streamId) in rows)
            {
                var late = false;
                lock (_sync)
                {
                    if (!range.Seen.Add(position))
                        continue;
                    late = _reported.Add(position);
                }

                if (late)
                    PostgresEventStoreLog.ReadHorizonSkippedEvent(_logger, position, streamId);
            }
        }
    }

    private async Task<List<long>> PositionsBetweenAsync(
        NpgsqlConnection connection,
        long after,
        long before,
        CancellationToken cancellationToken)
    {
        var rows = await RowsBetweenAsync(connection, after, before, cancellationToken).ConfigureAwait(false);
        return rows.Select(static row => row.Position).ToList();
    }

    private async Task<List<(long Position, string StreamId)>> RowsBetweenAsync(
        NpgsqlConnection connection,
        long after,
        long before,
        CancellationToken cancellationToken)
    {
        var sql = $"""
            SELECT global_position, stream_id
            FROM {_names.Events}
            WHERE global_position > @after AND global_position < @before
            ORDER BY global_position
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter("after", NpgsqlDbType.Bigint) { Value = after });
        command.Parameters.Add(new NpgsqlParameter("before", NpgsqlDbType.Bigint) { Value = before });
        var rows = new List<(long, string)>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            rows.Add((reader.GetInt64(0), reader.GetString(1)));
        return rows;
    }

    private static long Walk(long contiguous, IReadOnlyList<long> present)
    {
        var cursor = contiguous;
        foreach (var position in present.Order())
        {
            if (position == cursor + 1)
                cursor = position;
            else if (position > cursor + 1)
                break;
        }

        return cursor;
    }

    private static bool WaitsForever(TimeSpan grace) =>
        grace < TimeSpan.Zero || grace == TimeSpan.MaxValue;

    private readonly record struct Sample(long Contiguous, long? NextAfter, long Committed, IReadOnlyList<string> InProgress);

    private sealed class WatchedRange(long after, long before, HashSet<long> seen)
    {
        public long After { get; } = after;
        public long Before { get; } = before;
        public HashSet<long> Seen { get; } = seen;
    }
}
