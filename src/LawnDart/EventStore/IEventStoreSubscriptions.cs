namespace LawnDart.EventStore;

/// <summary>
/// Portable push / continuous-read subscription surface for event stores.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Subscribe"/> returns a continuous, globally ordered flow of
/// <see cref="SequencedEvent"/> values starting at <c>fromSequence</c> (inclusive).
/// Catch-up and live delivery are transparent to the caller — there is no client API
/// that signals “now live”.
/// </para>
/// <para>
/// Delivery is at-least-once. The client owns the durable resume cursor; reconnect with
/// <c>lastApplied + 1</c> after processing <c>lastApplied</c>. Implementations must not
/// silently drop events under back-pressure (bounded channel blocks or fails resumably).
/// </para>
/// <para>
/// This is the portable push contract, kept separate from <see cref="IEventStore"/> so
/// that stores without continuous-read support can omit it. Both shipped stores —
/// in-memory and SQL Server — implement it. Test for the interface before relying on
/// push delivery with a third-party store, and fall back to polling when absent.
/// </para>
/// </remarks>
public interface IEventStoreSubscriptions
{
    /// <summary>
    /// Subscribes to a continuous, globally ordered event flow starting at
    /// <paramref name="fromSequence"/> (inclusive). Catch-up and live are
    /// transparent to the caller.
    /// </summary>
    /// <param name="subscriberId">
    /// Stable identifier for logging and metrics. Not used as a server-side durable
    /// checkpoint for the portable contract (client-owned cursor).
    /// </param>
    /// <param name="fromSequence">
    /// Inclusive global sequence position to start from. Pass <c>0</c> (or the store’s
    /// first sequence) to replay from the beginning per backend conventions.
    /// </param>
    /// <param name="filter">
    /// Optional scope filter. <see langword="null"/> is treated as
    /// <see cref="EventSubscriptionFilter.All"/>.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the subscription and stops delivery (same effect as disposing the handle).
    /// </param>
    /// <returns>
    /// A handle whose <see cref="ISubscriptionHandle.Events"/> channel delivers matching
    /// events in non-decreasing <see cref="SequencedEvent.SequencePosition"/> order until
    /// cancel, dispose, or failure.
    /// </returns>
    ISubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default);
}
