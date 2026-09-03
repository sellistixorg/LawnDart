using System.Threading.Channels;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Portable subscription handle for <see cref="SqlServerEventStore"/> (poll-backed live).
/// </summary>
internal sealed class SqlServerSubscriptionHandle : ISubscriptionHandle
{
    private readonly Channel<SequencedEvent> _channel;
    private readonly Action<SqlServerSubscriptionHandle> _onDispose;
    private readonly CancellationTokenSource _linkedCts;
    private long _lastDelivered;
    private int _disposed;

    public SqlServerSubscriptionHandle(
        string subscriberId,
        EventSubscriptionFilter filter,
        int channelCapacity,
        long fromSequence,
        CancellationToken externalToken,
        Action<SqlServerSubscriptionHandle> onDispose)
    {
        SubscriberId = subscriberId;
        Filter = filter;
        FromSequence = fromSequence;
        _onDispose = onDispose;
        _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        _channel = Channel.CreateBounded<SequencedEvent>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false
        });
    }

    public string SubscriberId { get; }

    public EventSubscriptionFilter Filter { get; }

    public long FromSequence { get; }

    public ChannelReader<SequencedEvent> Events => _channel.Reader;

    public long LastDeliveredSequence => Interlocked.Read(ref _lastDelivered);

    internal CancellationToken CancellationToken => _linkedCts.Token;

    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    internal async ValueTask WriteAsync(SequencedEvent evt, CancellationToken cancellationToken)
    {
        await _channel.Writer.WriteAsync(evt, cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref _lastDelivered, evt.SequencePosition);
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return ValueTask.CompletedTask;

        try { _linkedCts.Cancel(); } catch (ObjectDisposedException) { /* ignore */ }
        _channel.Writer.TryComplete();
        _onDispose(this);
        _linkedCts.Dispose();
        return ValueTask.CompletedTask;
    }
}
