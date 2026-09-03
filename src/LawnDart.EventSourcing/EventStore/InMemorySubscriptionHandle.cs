using System.Threading.Channels;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.EventStore;

/// <summary>
/// Portable subscription handle for <see cref="InMemoryEventStore"/>.
/// </summary>
internal sealed class InMemorySubscriptionHandle : ISubscriptionHandle
{
    private readonly Channel<SequencedEvent> _channel;
    private readonly Action<InMemorySubscriptionHandle> _onDispose;
    private readonly CancellationTokenSource _linkedCts;
    private long _lastDelivered;
    private int _disposed;

    public InMemorySubscriptionHandle(
        string subscriberId,
        EventSubscriptionFilter filter,
        int channelCapacity,
        long fromSequence,
        CancellationToken externalToken,
        Action<InMemorySubscriptionHandle> onDispose)
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
        // Capacity 1 + DropOldest: wake signal only; never buffers events.
        Wake = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleWriter = false,
            SingleReader = true
        });
    }

    public string SubscriberId { get; }

    public EventSubscriptionFilter Filter { get; }

    /// <summary>Inclusive start sequence requested by the subscriber.</summary>
    public long FromSequence { get; }

    public ChannelReader<SequencedEvent> Events => _channel.Reader;

    public long LastDeliveredSequence => Interlocked.Read(ref _lastDelivered);

    internal CancellationToken CancellationToken => _linkedCts.Token;

    internal Channel<bool> Wake { get; }

    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    internal async ValueTask WriteAsync(SequencedEvent evt, CancellationToken cancellationToken)
    {
        await _channel.Writer.WriteAsync(evt, cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref _lastDelivered, evt.SequencePosition);
    }

    internal void Signal() => Wake.Writer.TryWrite(true);

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return ValueTask.CompletedTask;

        try { _linkedCts.Cancel(); } catch (ObjectDisposedException) { /* ignore */ }
        _channel.Writer.TryComplete();
        Wake.Writer.TryComplete();
        _onDispose(this);
        _linkedCts.Dispose();
        return ValueTask.CompletedTask;
    }
}
