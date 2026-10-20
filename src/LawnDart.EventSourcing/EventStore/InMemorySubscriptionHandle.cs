using System.Threading.Channels;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.EventStore;

/// <summary>
/// Log subscription handle for <see cref="InMemoryEventStore"/>. Yields records.
/// The typed adapter reads them into objects.
/// </summary>
internal sealed class InMemorySubscriptionHandle : IEventLogSubscriptionHandle
{
    private readonly Channel<RecordedEvent> _channel;
    private readonly Action<InMemorySubscriptionHandle> _onDispose;
    private readonly CancellationTokenSource _linkedCts;
    private long _lastDelivered;
    private int _disposed;
    private Exception? _fault;

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
        _channel = Channel.CreateBounded<RecordedEvent>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false
        });
    }

    public string SubscriberId { get; }

    public EventSubscriptionFilter Filter { get; }

    /// <summary>Inclusive start sequence requested by the subscriber.</summary>
    public long FromSequence { get; }

    public ChannelReader<RecordedEvent> Events => _channel.Reader;

    public long LastDeliveredSequence => Interlocked.Read(ref _lastDelivered);

    internal CancellationToken CancellationToken => _linkedCts.Token;

    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    internal void Fault(Exception exception)
    {
        _fault = exception;
        _channel.Writer.TryComplete(exception);
    }

    internal async ValueTask WriteAsync(RecordedEvent evt, CancellationToken cancellationToken)
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
        _channel.Writer.TryComplete(_fault);
        _onDispose(this);
        _linkedCts.Dispose();
        return ValueTask.CompletedTask;
    }
}
