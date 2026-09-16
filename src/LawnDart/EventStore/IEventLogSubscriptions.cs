namespace LawnDart.EventStore;

/// <summary>
/// Portable push / continuous-read surface for an <see cref="IEventLog"/>.
/// </summary>
/// <remarks>
/// Yields <see cref="RecordedEvent"/> frames. The typed session hydrates
/// <see cref="IEventStoreSubscriptions"/> subscribers; do not hydrate inside the
/// log implementation.
/// <para>
/// Same portable rules as <see cref="IEventStoreSubscriptions"/>: inclusive
/// <c>fromSequence</c>, catch-up then live with no phase signal, at-least-once,
/// client-owned cursor.
/// </para>
/// </remarks>
public interface IEventLogSubscriptions
{
    /// <summary>
    /// Subscribes to a continuous, globally ordered flow of recorded frames
    /// starting at <paramref name="fromSequence"/> (inclusive).
    /// </summary>
    IEventLogSubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default);
}
