using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;

namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// One Subscribe per manager/process: union live type filter, fan-out to attached
/// runners, private catch-up for handlers more than
/// <see cref="LightweightProjectionOptions.CatchUpJoinThreshold"/> behind the join point.
/// Does not own checkpoints — each runner still persists its own <c>StorageKey</c>.
/// </summary>
internal sealed class LightweightProjectionSharedPipe : IAsyncDisposable
{
    private readonly IEventStore _eventStore;
    private readonly IEventStoreSubscriptions? _subscriptions;
    private readonly LightweightProjectionOptions _options;
    private readonly string _subscriberId;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, Slot> _slots = new(StringComparer.Ordinal);

    private readonly object _gate = new();
    private TaskCompletionSource _activated = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _batchExpected;
    private bool _running;

    private ISubscriptionHandle? _handle;
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;
    private long _lastFanoutSequence = -1;
    private long _caughtUpSequence = -1;
    private long _joinSequence;
    private bool _faulted;
    private bool _firstDelivery;

    public LightweightProjectionSharedPipe(
        IEventStore eventStore,
        LightweightProjectionOptions options,
        string? contextName,
        ILogger? logger = null)
    {
        _eventStore = eventStore ?? throw new ArgumentNullException(nameof(eventStore));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _subscriptions = eventStore as IEventStoreSubscriptions;
        _subscriberId = $"lightweight:{contextName ?? "default"}:shared:n{options.NodeInstance}";
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public bool IsEnabled =>
        _options.UseSharedSubscribe
        && _options.UseSubscribe
        && _subscriptions is not null;

    public bool IsLive => IsEnabled && !_faulted && _handle is not null;

    public long CaughtUpSequence => Interlocked.Read(ref _caughtUpSequence);

    public long LastFanoutSequence => Interlocked.Read(ref _lastFanoutSequence);

    public long JoinSequence => Interlocked.Read(ref _joinSequence);

    public void BeginStartBatch(int expectedCount)
    {
        if (!IsEnabled)
            return;

        lock (_gate)
        {
            _batchExpected = Math.Max(0, expectedCount);
            if (!_activated.Task.IsCompleted)
                return;
            _activated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    public void EndStartBatch()
    {
        if (!IsEnabled)
            return;
        Activate();
    }

    public void Register(LightweightProjectionRunnerService runner, long lastApplied)
    {
        if (!IsEnabled)
            return;

        var slot = new Slot(runner, lastApplied);
        _slots[runner.StorageKey] = slot;

        lock (_gate)
        {
            if (_batchExpected == 0)
                Activate_NoLock();
        }
    }

    public async Task CompleteStartBatchAsync(int expectedCount, CancellationToken cancellationToken)
    {
        if (!IsEnabled)
            return;

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (_slots.Count < expectedCount && DateTime.UtcNow < deadline)
            await Task.Delay(5, cancellationToken).ConfigureAwait(false);

        Activate();
    }

    public void Unregister(string storageKey)
    {
        if (!_slots.TryRemove(storageKey, out var slot))
            return;

        slot.Inbox.Writer.TryComplete();
        lock (_gate)
        {
            if (_slots.IsEmpty)
                StopPump_NoLock();
        }
    }

    public Task WaitUntilActivatedAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled)
            return Task.CompletedTask;
        return _activated.Task.WaitAsync(cancellationToken);
    }

    public bool IsAttached(string storageKey) =>
        _slots.TryGetValue(storageKey, out var slot) && slot.Attached;

    public ChannelReader<SequencedEvent>? GetReader(string storageKey) =>
        _slots.TryGetValue(storageKey, out var slot) && slot.Attached
            ? slot.Inbox.Reader
            : null;

    public void ReportLastApplied(string storageKey, long lastApplied)
    {
        if (_slots.TryGetValue(storageKey, out var slot))
            slot.LastApplied = lastApplied;
    }

    public void Attach(string storageKey)
    {
        if (!_slots.TryGetValue(storageKey, out var slot))
            return;
        slot.Attached = true;
    }

    public void CancelForTests()
    {
        _logger.LogWarning("Shared pipe test-cancelled; runners will poll-recover");
        lock (_gate)
        {
            _faulted = true;
            StopPump_NoLock();
        }
    }

    public void Activate()
    {
        if (!IsEnabled)
        {
            _activated.TrySetResult();
            return;
        }

        lock (_gate)
            Activate_NoLock();
    }

    private void Activate_NoLock()
    {
        _batchExpected = 0;
        foreach (var slot in _slots.Values)
            slot.LastApplied = slot.Runner.GetObservabilitySnapshot().LastAppliedSequence;

        ClassifySlots_NoLock();

        if (!_running && _slots.Values.Any(s => s.Attached))
            StartPump_NoLock();

        _activated.TrySetResult();
    }

    private void ClassifySlots_NoLock()
    {
        var snapshot = _slots.Values.ToArray();
        if (snapshot.Length == 0)
            return;

        var maxApplied = snapshot.Max(s => s.LastApplied);
        var threshold = Math.Max(0, _options.CatchUpJoinThreshold);
        var live = snapshot
            .Where(s => maxApplied - s.LastApplied <= threshold)
            .ToArray();
        if (live.Length == 0)
            live = [snapshot.OrderByDescending(s => s.LastApplied).First()];

        var join = live.Min(s => s.LastApplied) + 1;
        _joinSequence = join;

        foreach (var slot in snapshot)
        {
            var farBehind = slot.LastApplied + threshold < join;
            slot.Attached = !farBehind;
        }

        _logger.LogInformation(
            "Shared pipe join={Join} (live={Live}, private={Private}, threshold={Threshold})",
            join,
            string.Join(",", live.Select(s => s.Runner.StorageKey)),
            string.Join(",", snapshot.Where(s => !s.Attached).Select(s => s.Runner.StorageKey)),
            threshold);
    }

    private void StartPump_NoLock()
    {
        if (_subscriptions is null)
            return;

        var attached = _slots.Values.Where(s => s.Attached).ToArray();
        if (attached.Length == 0)
            return;

        var fromSequence = Math.Max(0, attached.Min(s => s.LastApplied) + 1);
        var typeNames = _slots.Values
            .SelectMany(s => s.Runner.GetLiveEventTypeNames())
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        var filter = typeNames.Length == 0
            ? EventSubscriptionFilter.All()
            : EventSubscriptionFilter.ForQuery(Query.FromItems(QueryItem.ByType(typeNames)));

        try
        {
            StopPump_NoLock();
            _faulted = false;
            _firstDelivery = false;
            _pumpCts = new CancellationTokenSource();
            _handle = _subscriptions.Subscribe(_subscriberId, fromSequence, filter, _pumpCts.Token);
            _running = true;
            _lastFanoutSequence = fromSequence - 1;
            var ct = _pumpCts.Token;
            _pumpTask = Task.Run(() => PumpAsync(ct), CancellationToken.None);
            _logger.LogInformation(
                "Shared Subscribe from {From} (subscriber={Subscriber}, types={Types})",
                fromSequence, _subscriberId, typeNames.Length == 0 ? "*" : string.Join(",", typeNames));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Shared Subscribe failed; runners will poll");
            _faulted = true;
            StopPump_NoLock();
        }
    }

    private void StopPump_NoLock()
    {
        try { _pumpCts?.Cancel(); } catch (ObjectDisposedException) { /* ignore */ }
        try { _handle?.Dispose(); } catch { /* ignore */ }
        _handle = null;
        _pumpCts?.Dispose();
        _pumpCts = null;
        _running = false;
    }

    private async Task PumpAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested && _handle is not null)
            {
                var ingested = await DrainAndFanoutAsync(ct).ConfigureAwait(false);
                if (ingested > 0)
                    continue;

                if (_handle is null || _faulted)
                    break;

                await RecoveryPollAndFanoutAsync(ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // normal stop
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Shared pipe pump fault; runners will poll-recover");
            _faulted = true;
            lock (_gate)
                StopPump_NoLock();
        }
    }

    private async Task<int> DrainAndFanoutAsync(CancellationToken ct)
    {
        var handle = _handle;
        if (handle is null)
            return 0;

        var reader = handle.Events;
        if (!reader.TryRead(out var first))
        {
            if (await TryGetHeadAsync(ct).ConfigureAwait(false) is { } head
                && head > _lastFanoutSequence)
            {
                // Skip the idle-at-tail wait when Subscribe is empty but the store
                // has more events. Query-through-head still requires _firstDelivery.
                if (_firstDelivery
                    || head - _lastFanoutSequence > Math.Max(0L, _options.CatchUpFlushThreshold))
                    return 0;
            }

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var recovery = _options.SubscribeRecoveryPollInterval;
            if (recovery > TimeSpan.Zero)
                timeoutCts.CancelAfter(recovery);

            try
            {
                if (!await reader.WaitToReadAsync(timeoutCts.Token).ConfigureAwait(false))
                {
                    _faulted = true;
                    return 0;
                }
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return 0;
            }

            if (!reader.TryRead(out first))
                return 0;
        }

        var count = 0;
        var current = first;
        while (true)
        {
            await FanoutAsync(current, ct).ConfigureAwait(false);
            _firstDelivery = true;
            count++;
            if (count >= Math.Max(1, _options.BatchSize))
                break;
            if (!reader.TryRead(out current!))
                break;
        }

        return count;
    }

    private async Task RecoveryPollAndFanoutAsync(CancellationToken ct)
    {
        var from = _lastFanoutSequence + 1;
        var to = _lastFanoutSequence + _options.BatchSize;
        var queriedThroughHead = false;

        if (_firstDelivery && await TryGetHeadAsync(ct).ConfigureAwait(false) is { } head && head > _lastFanoutSequence)
        {
            to = head;
            queriedThroughHead = true;
        }

        var query = BuildUnionQuery();
        var batchCount = 0;
        await foreach (var se in _eventStore.ReadByQueryStreamAsync(
                           query, from, to, cancellationToken: ct).ConfigureAwait(false))
        {
            if (se.SequencePosition <= _lastFanoutSequence)
                continue;
            await FanoutAsync(se, ct).ConfigureAwait(false);
            batchCount++;
        }

        if (batchCount == 0 && queriedThroughHead)
        {
            var caught = to;
            if (caught > _caughtUpSequence)
                Interlocked.Exchange(ref _caughtUpSequence, caught);
            if (caught > _lastFanoutSequence)
                Interlocked.Exchange(ref _lastFanoutSequence, caught);
        }
    }

    private Query BuildUnionQuery()
    {
        var typeNames = _slots.Values
            .SelectMany(s => s.Runner.GetLiveEventTypeNames())
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        return typeNames.Length == 0
            ? Query.All()
            : Query.FromItems(QueryItem.ByType(typeNames));
    }

    private async Task FanoutAsync(SequencedEvent se, CancellationToken ct)
    {
        foreach (var slot in _slots.Values)
        {
            if (!slot.Attached || se.SequencePosition <= slot.LastApplied)
                continue;

            try
            {
                await slot.Inbox.Writer.WriteAsync(se, ct).ConfigureAwait(false);
            }
            catch (ChannelClosedException)
            {
                // runner unregistered
            }
        }

        if (se.SequencePosition > _lastFanoutSequence)
            Interlocked.Exchange(ref _lastFanoutSequence, se.SequencePosition);
    }

    private async Task<long?> TryGetHeadAsync(CancellationToken ct)
    {
        try
        {
            return await _eventStore.GetCurrentSequenceAsync(ct).ConfigureAwait(false);
        }
        catch (NotSupportedException)
        {
            return null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        lock (_gate)
            StopPump_NoLock();

        if (_pumpTask is not null)
        {
            try { await _pumpTask.ConfigureAwait(false); } catch { /* ignore */ }
        }

        foreach (var slot in _slots.Values)
            slot.Inbox.Writer.TryComplete();
        _slots.Clear();
        _activated.TrySetResult();
    }

    private sealed class Slot
    {
        public Slot(LightweightProjectionRunnerService runner, long lastApplied)
        {
            Runner = runner;
            LastApplied = lastApplied;
            Inbox = Channel.CreateBounded<SequencedEvent>(new BoundedChannelOptions(2048)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = true
            });
        }

        public LightweightProjectionRunnerService Runner { get; }
        public long LastApplied;
        public bool Attached;
        public Channel<SequencedEvent> Inbox { get; }
    }
}
