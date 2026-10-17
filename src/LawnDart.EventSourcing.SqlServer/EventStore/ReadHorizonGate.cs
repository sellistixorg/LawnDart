using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using LawnDart.EventSourcing.SqlServer;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Keeps global reads behind <c>MIN_ACTIVE_ROWVERSION()</c> until one unfinished
/// write has held that horizon longer than <see cref="SqlServerEventStoreOptions.ReadHorizonGracePeriod"/>.
/// </summary>
internal sealed class ReadHorizonGate
{
    private readonly string _qTable;
    private readonly SqlServerEventStoreOptions _options;
    private readonly ILogger? _logger;
    private readonly object _sync = new();
    private readonly List<SequenceGap> _gaps = new();
    private readonly HashSet<long> _visibleToInclusiveRead = new();
    private readonly HashSet<long> _reportedLate = new();

    private long? _blockerTransactionId;
    private long? _fallbackStableHead;
    private DateTime _fallbackSinceUtc;
    private bool _warned;
    private bool _dmvDenied;

    internal ReadHorizonGate(string qualifiedEventsTable, SqlServerEventStoreOptions options, ILogger? logger)
    {
        _qTable = qualifiedEventsTable;
        _options = options;
        _logger = logger;
    }

    internal async Task<ReadHorizon> ResolveAsync(
        SqlConnection connection,
        LateWatch watch,
        CancellationToken cancellationToken)
    {
        var (stable, committed) = await ReadHeadsAsync(connection, cancellationToken).ConfigureAwait(false);
        if (watch.Enabled)
            await ObserveLateRowsAsync(connection, watch, cancellationToken).ConfigureAwait(false);

        if (committed <= stable)
        {
            lock (_sync)
            {
                ResetBlocker();
                DropRollbackGaps(stable);
            }

            return new ReadHorizon(stable, Advanced: false);
        }

        if (WaitsForever(_options.ReadHorizonGracePeriod))
            return new ReadHorizon(stable, Advanced: false);

        var blocker = await TryReadBlockerAsync(connection, cancellationToken).ConfigureAwait(false);
        bool advanced;
        bool warn;
        long heldMs;
        long rawHead;
        string sessionId;
        string transactionId;
        lock (_sync)
        {
            heldMs = HoldMilliseconds(blocker, stable);
            var graceMs = _options.ReadHorizonGracePeriod.TotalMilliseconds;
            advanced = heldMs > graceMs;
            warn = false;
            rawHead = committed;
            if (advanced && !_warned)
            {
                _warned = true;
                warn = _logger is not null;
            }

            sessionId = blocker is { } known
                ? known.SessionId.ToString(CultureInfo.InvariantCulture)
                : "unknown";
            transactionId = blocker is { } tx
                ? tx.TransactionId.ToString(CultureInfo.InvariantCulture)
                : "unknown";
        }

        if (warn)
        {
            SqlServerEventStoreLog.ReadHorizonAdvanced(
                _logger!, heldMs, sessionId, transactionId, stable, rawHead);
        }

        if (!advanced)
            return new ReadHorizon(stable, Advanced: false);

        await CaptureGapsAsync(connection, stable, rawHead, cancellationToken).ConfigureAwait(false);
        return new ReadHorizon(rawHead, Advanced: true);
    }

    private async Task<(long Stable, long Committed)> ReadHeadsAsync(
        SqlConnection connection,
        CancellationToken cancellationToken)
    {
        var sql = $"""
            SELECT
                ISNULL((SELECT MAX(SequencePosition) FROM {_qTable} WITH (READPAST) WHERE [RowVersion] < MIN_ACTIVE_ROWVERSION()), 0),
                ISNULL((SELECT MAX(SequencePosition) FROM {_qTable} WITH (READPAST)), 0)
            """;
        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var readerDisposal = reader.ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return (0, 0);

        return (ReadInt64(reader, 0), ReadInt64(reader, 1));
    }

    private async Task<Blocker?> TryReadBlockerAsync(
        SqlConnection connection,
        CancellationToken cancellationToken)
    {
        if (_dmvDenied)
            return null;

        var sql = """
            SELECT TOP (1)
                st.session_id,
                at.transaction_id,
                ISNULL(DATEDIFF_BIG(millisecond, at.transaction_begin_time, GETDATE()), 0)
            FROM sys.dm_tran_active_transactions at
            INNER JOIN sys.dm_tran_session_transactions st ON st.transaction_id = at.transaction_id
            INNER JOIN sys.dm_tran_database_transactions dt ON dt.transaction_id = at.transaction_id
            WHERE dt.database_id = DB_ID()
              AND dt.database_transaction_begin_lsn IS NOT NULL
            ORDER BY at.transaction_begin_time ASC
            """;
        try
        {
            var command = new SqlCommand(sql, connection);
            await using var commandDisposal = command.ConfigureAwait(false);
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using var readerDisposal = reader.ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return null;

            var sessionId = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            var transactionId = ReadInt64(reader, 1);
            var heldMs = ReadInt64(reader, 2);
            return new Blocker(sessionId, transactionId, heldMs < 0 ? 0 : heldMs);
        }
        catch (SqlException)
        {
            _dmvDenied = true;
            return null;
        }
    }

    private long HoldMilliseconds(Blocker? blocker, long stableHead)
    {
        if (blocker is { } known)
        {
            if (_blockerTransactionId != known.TransactionId)
            {
                _blockerTransactionId = known.TransactionId;
                _warned = false;
                _fallbackStableHead = null;
            }

            return known.HeldMilliseconds;
        }

        var now = DateTime.UtcNow;
        if (_fallbackStableHead != stableHead)
        {
            _fallbackStableHead = stableHead;
            _fallbackSinceUtc = now;
            _blockerTransactionId = null;
            _warned = false;
            return 0;
        }

        var elapsed = (long)(now - _fallbackSinceUtc).TotalMilliseconds;
        return elapsed < 0 ? 0 : elapsed;
    }

    private async Task CaptureGapsAsync(
        SqlConnection connection,
        long stableHead,
        long committedHead,
        CancellationToken cancellationToken)
    {
        var sql = $"""
            WITH seqs AS (
                SELECT CAST(@Stable AS bigint) AS pos
                UNION ALL
                SELECT SequencePosition
                FROM {_qTable} WITH (READPAST)
                WHERE SequencePosition > @Stable AND SequencePosition <= @Committed
            )
            SELECT pos, nxt
            FROM (
                SELECT pos, LEAD(pos) OVER (ORDER BY pos) AS nxt
                FROM seqs
            ) d
            WHERE nxt > pos + 1
            """;
        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@Stable", stableHead);
        command.Parameters.AddWithValue("@Committed", committedHead);
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var readerDisposal = reader.ConfigureAwait(false);
        var found = new List<SequenceGap>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            found.Add(new SequenceGap(ReadInt64(reader, 0), ReadInt64(reader, 1)));

        lock (_sync)
        {
            foreach (var gap in found)
                RememberGap(gap);
        }
    }

    private async Task ObserveLateRowsAsync(
        SqlConnection connection,
        LateWatch watch,
        CancellationToken cancellationToken)
    {
        SequenceGap[] gaps;
        lock (_sync)
            gaps = _gaps.ToArray();

        if (gaps.Length == 0)
            return;

        var late = new List<LateRow>();
        foreach (var gap in gaps)
        {
            var sql = $"""
                SELECT SequencePosition, StreamId
                FROM {_qTable} WITH (READPAST)
                WHERE SequencePosition > @After AND SequencePosition < @Before
                ORDER BY SequencePosition
                """;
            var command = new SqlCommand(sql, connection);
            await using var commandDisposal = command.ConfigureAwait(false);
            command.Parameters.AddWithValue("@After", gap.After);
            command.Parameters.AddWithValue("@Before", gap.Before);
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using var readerDisposal = reader.ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                late.Add(new LateRow(
                    ReadInt64(reader, 0),
                    reader.IsDBNull(1) ? "" : reader.GetString(1)));
            }
        }

        if (late.Count == 0)
            return;

        List<LateRow> toLog = new();
        lock (_sync)
        {
            foreach (var row in late)
            {
                NoteFilled(row.Sequence);
                if (!watch.HeadCheck)
                {
                    var from = watch.FromSequence ?? 0;
                    if (from <= row.Sequence)
                    {
                        _visibleToInclusiveRead.Add(row.Sequence);
                        continue;
                    }
                }

                if (_visibleToInclusiveRead.Contains(row.Sequence) || !_reportedLate.Add(row.Sequence))
                    continue;

                toLog.Add(row);
            }
        }

        if (_logger is null)
            return;

        foreach (var row in toLog)
            SqlServerEventStoreLog.ReadHorizonSkippedEvent(_logger, row.Sequence, row.StreamId);
    }

    private void RememberGap(SequenceGap gap)
    {
        if (gap.Before <= gap.After + 1)
            return;

        for (var i = 0; i < _gaps.Count; i++)
        {
            var existing = _gaps[i];
            if (gap.After <= existing.Before && gap.Before >= existing.After)
            {
                _gaps[i] = new SequenceGap(
                    Math.Min(existing.After, gap.After),
                    Math.Max(existing.Before, gap.Before));
                return;
            }
        }

        _gaps.Add(gap);
    }

    private void NoteFilled(long sequence)
    {
        for (var i = _gaps.Count - 1; i >= 0; i--)
        {
            var gap = _gaps[i];
            if (sequence <= gap.After || sequence >= gap.Before)
                continue;

            _gaps.RemoveAt(i);
            RememberGap(new SequenceGap(gap.After, sequence));
            RememberGap(new SequenceGap(sequence, gap.Before));
        }
    }

    private void DropRollbackGaps(long stableHead)
    {
        _gaps.RemoveAll(gap => gap.Before <= stableHead + 1);
    }

    private void ResetBlocker()
    {
        _blockerTransactionId = null;
        _fallbackStableHead = null;
        _warned = false;
    }

    private static bool WaitsForever(TimeSpan grace) =>
        grace < TimeSpan.Zero || grace == TimeSpan.MaxValue;

    private static long ReadInt64(SqlDataReader reader, int ordinal)
    {
        var value = reader.GetValue(ordinal);
        return value is long number ? number : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private readonly record struct SequenceGap(long After, long Before);

    private readonly record struct Blocker(int SessionId, long TransactionId, long HeldMilliseconds);

    private readonly record struct LateRow(long Sequence, string StreamId);
}

/// <summary>Head a global reader may treat as caught up.</summary>
internal readonly record struct ReadHorizon(long Head, bool Advanced);

/// <summary>
/// How a caller participates in late-commit detection.
/// <see cref="Head"/> is <c>GetCurrentSequenceAsync</c>.
/// <see cref="Read"/> is an event read. <see cref="None"/> does not watch.
/// </summary>
internal readonly record struct LateWatch(bool Enabled, bool HeadCheck, long? FromSequence)
{
    internal static LateWatch None { get; } = new(false, false, null);

    internal static LateWatch Head { get; } = new(true, true, null);

    internal static LateWatch Read(long? fromSequence) => new(true, false, fromSequence);
}
