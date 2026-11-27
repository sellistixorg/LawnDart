using System.Globalization;
using Npgsql;
using NpgsqlTypes;

namespace LawnDart.EventSourcing.Postgres.EventStore;

public sealed partial class PostgresEventStore
{
    // First position with no range table that this store knows of. 0 until the
    // first read of the catalog, so the first appends check for themselves.
    private long _partitionLimit;

    // Newest position this store has seen from its own appends or a wake-up.
    private long _knownHead;

    // 1 while a background range check runs. Only one runs at a time.
    private int _rangesAheadRunning;

    // After a background range check fails or makes no progress, no new one starts
    // before this Environment.TickCount64 value, so a lasting failure logs once per
    // pause instead of once per append. Appends still make the range they need.
    private long _rangesAheadPausedUntil;

    // Calls to the range function made by background checks. Read by tests.
    private long _rangesAheadCalls;

    /// <summary>How long background range checks pause after one fails or makes no progress.</summary>
    internal static readonly TimeSpan RangesAheadPause = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Creates the events and event_tags range tables up to the range that holds the newest
    /// position handed out, plus two empty ranges above it. Does nothing when position ranges are off.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    /// The store already does this on its own: <see cref="InitializeSchemaAsync"/> runs it, and
    /// appends start it in the background as the newest position nears the last range.
    /// Call it from an admin tool or after a bulk load that set the position sequence.
    /// Safe to run from many nodes at once. Uses <see cref="PostgresEventStoreOptions.ConnectionString"/>.
    /// </remarks>
    public async Task EnsurePartitionsAsync(CancellationToken cancellationToken = default)
        => await EnsureRangesAsync(_dataSource, position: null, cancellationToken).ConfigureAwait(false);

    /// <summary>Test seam. The first position with no range table that this store knows of.</summary>
    internal long KnownPartitionLimit => Volatile.Read(ref _partitionLimit);

    /// <summary>Test seam. True while background range checks pause after a failure or no progress.</summary>
    internal bool RangesAheadPaused => Environment.TickCount64 < Volatile.Read(ref _rangesAheadPausedUntil);

    /// <summary>Test seam. How many times background checks called the range function.</summary>
    internal long RangesAheadCalls => Interlocked.Read(ref _rangesAheadCalls);

    /// <summary>Test seam. Completes when no background range check is running.</summary>
    internal async Task WaitForRangesAheadAsync(CancellationToken cancellationToken = default)
    {
        while (Volatile.Read(ref _rangesAheadRunning) != 0)
            await Task.Delay(10, cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureRangesAsync(NpgsqlDataSource source, long? position, CancellationToken cancellationToken)
    {
        if (!_options.UsesPositionPartitions)
            return;

        var connection = await source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        var command = new NpgsqlCommand(
            $"SELECT {_names.EnsurePartitions}(@position, {PostgresSchema.RangesAhead.ToString(CultureInfo.InvariantCulture)})",
            connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.Add(new NpgsqlParameter("position", NpgsqlDbType.Bigint) { Value = (object?)position ?? DBNull.Value });
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (result is long limit)
            RaiseLimit(limit);
    }

    private void RaiseLimit(long limit)
    {
        long observed;
        do
        {
            observed = Volatile.Read(ref _partitionLimit);
            if (limit <= observed)
                return;
        }
        while (Interlocked.CompareExchange(ref _partitionLimit, limit, observed) != observed);
    }

    private void NoteHead(long position)
    {
        long observed;
        do
        {
            observed = Volatile.Read(ref _knownHead);
            if (position <= observed)
                return;
        }
        while (Interlocked.CompareExchange(ref _knownHead, position, observed) != observed);
    }

    /// <summary>
    /// The value for <c>p_partition_limit</c>: null when ranges are off, otherwise the
    /// first position this store has not seen a range for. Past it, the append checks first.
    /// </summary>
    private object PartitionLimitParameter()
        => _options.UsesPositionPartitions ? Volatile.Read(ref _partitionLimit) : DBNull.Value;

    /// <summary>
    /// A fresh stamp for one append. The floor is the newest position seen so far;
    /// the time is taken later, when the first attempt builds its statement.
    /// </summary>
    private AppendStamp NewAppendStamp() => new(Math.Max(0, Volatile.Read(ref _knownHead)), AppendTimeForTests);

    /// <summary>
    /// Test seam. When set, every append writes this time, so two calls with the same
    /// event ids act like two attempts of one append.
    /// </summary>
    internal DateTime? AppendTimeForTests { get; set; }

    /// <summary>
    /// After an append, starts a background check when fewer than two empty ranges are
    /// known above the range that holds <paramref name="newest"/>. Never blocks the append.
    /// </summary>
    private void KeepRangesAhead(long newest)
    {
        NoteHead(newest);
        if (!_options.UsesPositionPartitions || newest < 1)
            return;
        StartRangesAhead();
    }

    /// <summary>
    /// True when the store does not yet know of two empty ranges above the range
    /// that holds <paramref name="newest"/>.
    /// </summary>
    private bool NeedsRangesAhead(long newest)
    {
        if (newest < 1)
            return false;
        var size = _options.PartitionSize;
        var range = (newest - 1) / size;
        var needed = range > (long.MaxValue / size) - PostgresSchema.RangesAhead - 2
            ? long.MaxValue
            : ((range + PostgresSchema.RangesAhead + 1) * size) + 1;
        return Volatile.Read(ref _partitionLimit) < needed;
    }

    /// <summary>
    /// One background check at a time. It keeps going while appends move the newest
    /// position on, so the last append before a quiet spell still gets its two empty
    /// ranges even when its own start lost to a check that was already running.
    /// </summary>
    private void StartRangesAhead()
    {
        while (NeedsRangesAhead(Volatile.Read(ref _knownHead)))
        {
            if (Environment.TickCount64 < Volatile.Read(ref _rangesAheadPausedUntil))
                return;
            if (Interlocked.CompareExchange(ref _rangesAheadRunning, 1, 0) != 0)
                return;

            _ = Task.Run(RunRangesAheadAsync);
            return;
        }
    }

    private async Task RunRangesAheadAsync()
    {
        long head = 0;
        try
        {
            while (NeedsRangesAhead(head = Volatile.Read(ref _knownHead)))
            {
                var before = Volatile.Read(ref _partitionLimit);
                Interlocked.Increment(ref _rangesAheadCalls);
                await EnsureRangesAsync(_dataSource, head, CancellationToken.None).ConfigureAwait(false);
                // No progress means the function cannot do more for now: the position this
                // store saw is above what the sequence handed out, as after a restore or a
                // sequence reset. Pause like a failure, so the restart below and later
                // appends do not call it again in a tight loop. Appends past the known
                // ranges still make the range they need.
                if (Volatile.Read(ref _partitionLimit) <= before)
                {
                    Volatile.Write(ref _rangesAheadPausedUntil, Environment.TickCount64 + (long)RangesAheadPause.TotalMilliseconds);
                    if (_logger is not null)
                        PostgresEventStoreLog.PartitionsAheadNoProgress(_logger, head, (int)RangesAheadPause.TotalSeconds);
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Volatile.Write(ref _rangesAheadPausedUntil, Environment.TickCount64 + (long)RangesAheadPause.TotalMilliseconds);
            if (_logger is not null)
                PostgresEventStoreLog.PartitionsAheadFailed(_logger, ex, head, (int)RangesAheadPause.TotalSeconds);
        }
        finally
        {
            Volatile.Write(ref _rangesAheadRunning, 0);
        }

        // An append that noted a newer position while this check was finishing
        // could not start its own. Start again for it.
        StartRangesAhead();
    }
}
