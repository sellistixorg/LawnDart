using System.Threading.Channels;

namespace LawnDart.EventStore;

/// <summary>
/// Typed subscription handle: hydrates log frames. A fail-closed hydrate
/// completes the channel with the error and does not advance
/// <see cref="LastDeliveredSequence"/>.
/// </summary>
internal sealed class HydratingSubscriptionHandle : ISubscriptionHandle
{
    private readonly Channel<SequencedEvent> _channel;
    private readonly IEventLogSubscriptionHandle _logHandle;
    private readonly CancellationTokenSource _linkedCts;
    private long _lastDelivered;
    private int _disposed;

    internal HydratingSubscriptionHandle(
        EventSession session,
        IEventLogSubscriptionHandle logHandle,
        int channelCapacity,
        CancellationToken externalToken)
    {
        ArgumentNullException.ThrowIfNull(session);
        _logHandle = logHandle ?? throw new ArgumentNullException(nameof(logHandle));
        SubscriberId = logHandle.SubscriberId;
        _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        _channel = Channel.CreateBounded<SequencedEvent>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false
        });

        _ = Task.Run(() => PumpAsync(session), CancellationToken.None);
    }

    public string SubscriberId { get; }

    public ChannelReader<SequencedEvent> Events => _channel.Reader;

    public long LastDeliveredSequence => Interlocked.Read(ref _lastDelivered);

    private async Task PumpAsync(EventSession session)
    {
        var ct = _linkedCts.Token;
        try
        {
            while (await _logHandle.Events.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                while (_logHandle.Events.TryRead(out var frame))
                {
                    SequencedEvent sequenced;
                    try
                    {
                        sequenced = session.Hydrate(frame);
                    }
                    catch (Exception ex)
                    {
                        _channel.Writer.TryComplete(ex);
                        return;
                    }

                    await _channel.Writer.WriteAsync(sequenced, ct).ConfigureAwait(false);
                    Interlocked.Exchange(ref _lastDelivered, frame.SequencePosition);
                }
            }

            _channel.Writer.TryComplete();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _channel.Writer.TryComplete();
        }
        catch (ChannelClosedException)
        {
            _channel.Writer.TryComplete();
        }
        catch (Exception ex)
        {
            _channel.Writer.TryComplete(ex);
        }
        finally
        {
            await DisposeAsync().ConfigureAwait(false);
        }
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try { _linkedCts.Cancel(); } catch (ObjectDisposedException) { /* ignore */ }
        _channel.Writer.TryComplete();
        await _logHandle.DisposeAsync().ConfigureAwait(false);
        _linkedCts.Dispose();
    }
}
