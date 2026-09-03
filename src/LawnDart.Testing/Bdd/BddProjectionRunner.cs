using LawnDart.EventStore;
using LawnDart;

namespace LawnDart.Testing.Bdd;

/// <summary>
/// Basic in-process projection runner for tests.
/// This intentionally mirrors demo-style projection execution, without the hosted
/// runner's checkpointing or coordination.
/// </summary>
public sealed class BddProjectionRunner
{
    private readonly List<Action<IEvent>> _appliers = [];
    private readonly Dictionary<Type, Dictionary<string, object>> _views = [];

    /// <summary>
    /// Registers a projection callback that will be invoked for each projected event.
    /// </summary>
    public BddProjectionRunner Register(Action<IEvent> apply)
    {
        ArgumentNullException.ThrowIfNull(apply);
        _appliers.Add(apply);
        return this;
    }

    /// <summary>
    /// Stores or replaces a projected view for later assertions.
    /// </summary>
    public void UpsertView<TView>(string key, TView view)
        where TView : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(view);

        var viewType = typeof(TView);
        if (!_views.TryGetValue(viewType, out var bucket))
        {
            bucket = new Dictionary<string, object>(StringComparer.Ordinal);
            _views[viewType] = bucket;
        }

        bucket[key] = view;
    }

    /// <summary>
    /// Retrieves a typed projected view by key.
    /// </summary>
    public TView? GetView<TView>(string key)
        where TView : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var viewType = typeof(TView);
        if (!_views.TryGetValue(viewType, out var bucket))
        {
            return null;
        }

        return bucket.TryGetValue(key, out var view) ? view as TView : null;
    }

    /// <summary>
    /// Returns all projected views for the given type.
    /// </summary>
    public IReadOnlyList<TView> GetAll<TView>()
        where TView : class
    {
        var viewType = typeof(TView);
        if (!_views.TryGetValue(viewType, out var bucket))
        {
            return [];
        }

        return bucket.Values.Cast<TView>().ToArray();
    }

    /// <summary>
    /// Projects all events from a stream in version order.
    /// </summary>
    public async Task ProjectStreamAsync(
        IEventStore store,
        string streamId,
        CancellationToken cancellationToken = default)
    {
        var events = await store.ReadStreamAsync(streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        Apply(events);
    }

    /// <summary>
    /// Projects events matching a query in sequence-position order.
    /// </summary>
    public async Task ProjectQueryAsync(
        IEventStore store,
        Query query,
        CancellationToken cancellationToken = default)
    {
        var result = await store.ReadByQueryAsync(query, cancellationToken: cancellationToken).ConfigureAwait(false);
        Apply(result.Events);
    }

    /// <summary>
    /// Projects only events with sequence position greater than <paramref name="afterSequencePosition"/>.
    /// </summary>
    public async Task ProjectQuerySinceAsync(
        IEventStore store,
        Query query,
        long afterSequencePosition,
        CancellationToken cancellationToken = default)
    {
        var result = await store.ReadByQueryAsync(
                query,
                fromSequencePosition: afterSequencePosition + 1,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        Apply(result.Events);
    }

    /// <summary>
    /// Polls until <paramref name="predicate"/> is true or the timeout is reached.
    /// </summary>
    public static async Task AwaitAsync(
        Func<bool> predicate,
        TimeSpan timeout,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(100);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (predicate())
            {
                return;
            }

            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException($"Condition was not met within {timeout}.");
    }

    private void Apply(IEnumerable<SequencedEvent> events)
    {
        foreach (var sequencedEvent in events.OrderBy(e => e.SequencePosition))
        {
            foreach (var apply in _appliers)
            {
                apply(sequencedEvent.Event);
            }
        }
    }
}
