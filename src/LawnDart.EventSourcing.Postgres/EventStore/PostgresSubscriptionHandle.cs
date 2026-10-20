using System.Threading.Channels;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// Log subscription handle. Yields stored records. The typed adapter reads them into objects.
/// </summary>
internal sealed class PostgresSubscriptionHandle : IEventLogSubscriptionHandle
{
    private readonly Channel<RecordedEvent> _channel;
    private readonly Action<PostgresSubscriptionHandle> _onDispose;
    private readonly CancellationTokenSource _linked;
    private long _lastDelivered;
    private int _disposed;
    private Exception? _fault;

    public PostgresSubscriptionHandle(
        string subscriberId,
        EventSubscriptionFilter filter,
        int capacity,
        long fromSequence,
        CancellationToken externalToken,
        Action<PostgresSubscriptionHandle> onDispose)
    {
        SubscriberId = subscriberId;
        Filter = filter;
        FromSequence = fromSequence;
        _onDispose = onDispose;
        _linked = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        _channel = Channel.CreateBounded<RecordedEvent>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false
        });
    }

    public string SubscriberId { get; }

    public EventSubscriptionFilter Filter { get; }

    public long FromSequence { get; }

    public ChannelReader<RecordedEvent> Events => _channel.Reader;

    public long LastDeliveredSequence => Interlocked.Read(ref _lastDelivered);

    internal CancellationToken CancellationToken => _linked.Token;

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

        try { _linked.Cancel(); } catch (ObjectDisposedException) { }
        _channel.Writer.TryComplete(_fault);
        _onDispose(this);
        _linked.Dispose();
        return ValueTask.CompletedTask;
    }
}
