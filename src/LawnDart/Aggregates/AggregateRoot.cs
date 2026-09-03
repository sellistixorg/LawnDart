using LawnDart;
using LawnDart.Snapshots;

namespace LawnDart.Aggregates;

/// <summary>
/// Base class for aggregate roots (non-generic).
/// </summary>
public abstract class AggregateRoot
{
    /// <summary>
    /// The stream ID for this aggregate instance.
    /// </summary>
    public string StreamId { get; protected set; } = string.Empty;

    /// <summary>
    /// Current version of the aggregate (for optimistic concurrency).
    /// Incremented by <c>Apply</c> for each pending event, and synced to the store-assigned
    /// version by <c>AggregateRepository</c> after a successful flush.
    /// </summary>
    public long Version { get; protected set; }

    /// <summary>
    /// The version last confirmed written to the store.
    /// <list type="bullet">
    ///   <item><c>-1</c> — aggregate was obtained via <c>CreateAsync</c> and has never been flushed.
    ///     Matches the <c>-1</c> sentinel used by <c>AppendAsync(expectedVersion: -1)</c>, which means
    ///     "stream must not exist yet". This enforces that two concurrent creates to the same ID will
    ///     result in a <c>ConcurrencyException</c> for the second one.</item>
    ///   <item><c>N ≥ 0</c> — aggregate was loaded via <c>GetAsync</c> or was flushed at least once.
    ///     <c>AppendAsync</c> will assert the stream is currently at version N.</item>
    /// </list>
    /// <c>AggregateRepository</c> sets this via <see cref="SetCommittedVersion"/> — it must not
    /// be set directly by application code.
    /// </summary>
    public long CommittedVersion { get; private set; } = -1;

    /// <summary>
    /// Called by <c>AggregateRepository</c> to record the last version confirmed in the store.
    /// Sets <see cref="CommittedVersion"/>. Must not be called by application code.
    /// </summary>
    public void SetCommittedVersion(long version) => CommittedVersion = version;

    /// <summary>
    /// Events that have been applied but not yet persisted.
    /// </summary>
    public abstract IEnumerable<IEvent> PendingEvents { get; }

    /// <summary>
    /// Clears pending events. Called after events have been persisted.
    /// </summary>
    public abstract void ClearPendingEvents();

    /// <summary>
    /// Sets the stream ID. Called when loading from event store.
    /// </summary>
    public void SetStreamId(string streamId)
    {
        StreamId = streamId ?? throw new ArgumentNullException(nameof(streamId));
    }

    /// <summary>
    /// Sets the version. Called when loading from event store.
    /// </summary>
    public void SetVersion(long version)
    {
        Version = version;
    }
    
    /// <summary>
    /// Handles a command and applies resulting events.
    /// This method must be implemented by derived aggregate types.
    /// </summary>
    /// <typeparam name="TCommand">The command type.</typeparam>
    /// <param name="command">The command to handle.</param>
    /// <param name="cancellationToken">Token used to cancel domain work.</param>
    /// <returns>Task representing the async operation.</returns>
    public abstract Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default) where TCommand : ICommand;

    // ── Snapshot hooks ───────────────────────────────────────────────────────
    // These virtual methods allow the repository (which works with the non-generic
    // AggregateRoot base) to invoke type-aware snapshot operations without knowing TState.
    // The generic AggregateRoot<TState> overrides both with type-safe implementations.

    /// <summary>
    /// Attempts to restore aggregate state from the most recent snapshot in <paramref name="store"/>.
    /// Returns <see cref="SnapshotInfo"/> if a snapshot was successfully restored, or <c>null</c>
    /// if no snapshot exists (triggering a full event replay in the repository).
    /// </summary>
    /// <remarks>
    /// The base implementation always returns <c>null</c> (no snapshot support).
    /// <see cref="AggregateRoot{TState}"/> overrides this to perform the actual load and state restore.
    /// After restore, the repository replays only delta events from <c>info.Version + 1</c>.
    /// </remarks>
    public virtual Task<SnapshotInfo?> TryRestoreFromSnapshotAsync(
        ISnapshotStore store, CancellationToken ct) =>
        Task.FromResult<SnapshotInfo?>(null);

    /// <summary>
    /// Writes a snapshot of the current aggregate state to <paramref name="store"/>.
    /// Called fire-and-forget from the repository after a successful append when the
    /// registered <see cref="ISnapshotStrategy"/> returns <c>true</c>.
    /// </summary>
    /// <remarks>
    /// The base implementation is a no-op.
    /// <see cref="AggregateRoot{TState}"/> overrides this to serialize and persist <c>State</c>.
    /// </remarks>
    public virtual Task TrySaveSnapshotAsync(
        ISnapshotStore store, long globalSequence, CancellationToken ct) =>
        Task.CompletedTask;
}

/// <summary>
/// Base class for aggregate roots that handle commands and emit events.
/// </summary>
/// <typeparam name="TState">The state type for this aggregate.</typeparam>
public abstract partial class AggregateRoot<TState> : AggregateRoot where TState : IState, new()
{
    private readonly List<IEvent> _pendingEvents = new();

    /// <summary>
    /// The current state of the aggregate.
    /// </summary>
    public TState State { get; protected set; } = new();

    /// <summary>
    /// Events that have been applied but not yet persisted.
    /// </summary>
    public override IEnumerable<IEvent> PendingEvents => _pendingEvents.AsReadOnly();

    /// <summary>
    /// Handles a command and applies resulting events.
    /// </summary>
    /// <typeparam name="TCommand">The command type.</typeparam>
    /// <param name="command">The command to handle.</param>
    /// <param name="cancellationToken">Token used to cancel domain work.</param>
    /// <returns>Task representing the async operation.</returns>
    public override abstract Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies an event to the aggregate state. This should be called from HandleAsync implementations.
    /// </summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="event">The event to apply.</param>
    protected void Apply<TEvent>(TEvent @event) where TEvent : IEvent
    {
        if (@event == null)
            throw new ArgumentNullException(nameof(@event));

        // Apply to state (required override on the derived aggregate)
        ApplyEventToState(@event);

        // Track pending event
        _pendingEvents.Add(@event);

        // Increment version
        Version++;
    }

    /// <summary>
    /// Applies an event to the aggregate state. Derived aggregates must fold the event into
    /// <see cref="State"/>. An empty virtual default would let <see cref="Apply{TEvent}"/>
    /// persist and increment <see cref="AggregateRoot.Version"/> with no state change.
    /// </summary>
    /// <param name="event">The event to apply.</param>
    protected abstract void ApplyEventToState(IEvent @event);

    /// <summary>
    /// Replays events to rebuild aggregate state. Used when loading from event store.
    /// </summary>
    /// <param name="events">Events to replay.</param>
    public void ReplayEvents(IEnumerable<IEvent> events)
    {
        if (events == null)
            throw new ArgumentNullException(nameof(events));

        foreach (var @event in events)
        {
            ApplyEventToState(@event);
            Version++;
        }
    }

    /// <summary>
    /// Clears pending events. Called after events have been persisted.
    /// </summary>
    public override void ClearPendingEvents()
    {
        _pendingEvents.Clear();
    }

    // ── Snapshot hooks (type-aware overrides) ────────────────────────────────

    /// <summary>
    /// Attempts to restore this aggregate's state from the most recent snapshot.
    /// Sets <see cref="AggregateRoot.Version"/> to the snapshot version so the repository
    /// can replay only delta events from <c>info.Version + 1</c>.
    /// </summary>
    public override async Task<SnapshotInfo?> TryRestoreFromSnapshotAsync(
        ISnapshotStore store, CancellationToken ct)
    {
        var (state, info) = await store.LoadSnapshotAsync<TState>(StreamId, ct);
        if (info == null || state == null) return null;

        State = state;
        SetVersion(info.Version);
        return info;
    }

    /// <summary>
    /// Serializes the current <see cref="State"/> and writes it to <paramref name="store"/>
    /// as a snapshot at the current <see cref="AggregateRoot.Version"/> and <paramref name="globalSequence"/>.
    /// </summary>
    public override Task TrySaveSnapshotAsync(
        ISnapshotStore store, long globalSequence, CancellationToken ct) =>
        store.SaveSnapshotAsync<TState>(StreamId, Version, globalSequence, State, ct);
}

