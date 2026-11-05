using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Dcb;

/// <summary>
/// Repository for dynamic consistency boundary entities.
/// A decision reads a query, folds the matching events into state, and appends under that same query.
/// </summary>
public interface IDcbRepository
{
    /// <summary>
    /// Rebuilds state from events that match <paramref name="query"/>.
    /// </summary>
    /// <typeparam name="TState">State type to rebuild.</typeparam>
    /// <param name="query">
    /// Decision query. Items combine with OR. Inside one item, tags combine with AND and event types combine with OR.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Rebuilt state.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="query"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="query"/> has no items, an item has no event types and no tags,
    /// or an item sets <see cref="QueryItem.PartitionFilter"/>.
    /// </exception>
    Task<TState> GetStateAsync<TState>(
        Query query,
        CancellationToken cancellationToken = default) where TState : IState, new();

    /// <summary>
    /// Rebuilds state from events that match <paramref name="query"/> as of a specific point in time.
    /// Does not use snapshots, so the point in time is the log.
    /// </summary>
    /// <typeparam name="TState">State type to rebuild.</typeparam>
    /// <param name="query">Decision query. Same shape as <see cref="GetStateAsync{TState}"/>.</param>
    /// <param name="asOfTimestamp">Include only events with <see cref="EventMetadata.Timestamp"/> &lt;= this value (inclusive).</param>
    /// <param name="asOfSequencePosition">Include only events with SequencePosition &lt;= this value (inclusive).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Rebuilt state at the specified point in time.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="query"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="query"/> has no items, an item has no event types and no tags,
    /// or an item sets <see cref="QueryItem.PartitionFilter"/>.
    /// </exception>
    Task<TState> GetStateAtAsync<TState>(
        Query query,
        DateTime? asOfTimestamp = null,
        long? asOfSequencePosition = null,
        CancellationToken cancellationToken = default) where TState : IState, new();

    /// <summary>
    /// Gets the last sequence position for events that match <paramref name="query"/>.
    /// Used as a checkpoint for optimistic concurrency. Backed by
    /// <see cref="IEventStore.GetMaxSequencePositionAsync"/>.
    /// </summary>
    /// <param name="query">Decision query. Same shape as <see cref="GetStateAsync{TState}"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Last sequence position, or 0 if no events match.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="query"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="query"/> has no items, an item has no event types and no tags,
    /// or an item sets <see cref="QueryItem.PartitionFilter"/>.
    /// </exception>
    Task<long> GetLastSequencePositionAsync(
        Query query,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends events with automatic tag generation, metadata enrichment, and authorization.
    /// Each event is written with only its own tags, plus the tag provider's tags for that
    /// event and the tenant tag when tenant isolation is on.
    /// </summary>
    /// <param name="events">Events to append, each with its own tags.</param>
    /// <param name="condition">Optional append condition for optimistic concurrency.</param>
    /// <param name="commandMetadata">Optional command metadata for enrichment and authorization.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Sequence positions assigned to the events.</returns>
    Task<IReadOnlyList<long>> AppendWithContextAsync(
        IEnumerable<TaggedEvent> events,
        AppendCondition? condition = null,
        CommandMetadata? commandMetadata = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new entity and stores <paramref name="query"/> on it.
    /// Does not read the event store.
    /// </summary>
    /// <typeparam name="TEntity">Entity type.</typeparam>
    /// <param name="query">Decision query. Same shape as <see cref="GetStateAsync{TState}"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>New entity instance.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="query"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="query"/> has no items, an item has no event types and no tags,
    /// or an item sets <see cref="QueryItem.PartitionFilter"/>.
    /// </exception>
    Task<TEntity> CreateEntityAsync<TEntity>(
        Query query,
        CancellationToken cancellationToken = default) where TEntity : DcbEntity, new();

    /// <summary>
    /// Loads an entity for <paramref name="query"/>, or creates one when no matching events exist.
    /// Stores that query on the entity. An event that matches two items is applied once.
    /// </summary>
    /// <typeparam name="TEntity">Entity type.</typeparam>
    /// <param name="query">Decision query. Same shape as <see cref="GetStateAsync{TState}"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Existing or new entity instance.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="query"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="query"/> has no items, an item has no event types and no tags,
    /// or an item sets <see cref="QueryItem.PartitionFilter"/>.
    /// </exception>
    Task<TEntity> GetOrCreateEntityAsync<TEntity>(
        Query query,
        CancellationToken cancellationToken = default) where TEntity : DcbEntity, new();

    /// <summary>
    /// Handles a command on a dynamic consistency boundary entity with optional authorization checking.
    /// This method performs authorization (if enabled), dispatches the command
    /// (<c>HandleAsync</c> when overridden, otherwise closed <c>Handle(TCommand)</c>),
    /// and persists events.
    /// The append condition is <see cref="AppendCondition.FailIfMatches"/> on
    /// <see cref="DcbEntity.DecisionQuery"/>.
    /// If <paramref name="commandMetadata"/> is <c>null</c>, metadata is captured automatically
    /// from the registered <see cref="IMetadataProvider"/>.
    /// </summary>
    /// <remarks>
    /// Each event the command emits should match at least one item of <see cref="DcbEntity.DecisionQuery"/>.
    /// The fence position is the last appended sequence, including an event that matches no item.
    /// A matching event that lands at or before that position does not fail the append.
    /// </remarks>
    /// <typeparam name="TEntity">Entity type.</typeparam>
    /// <typeparam name="TCommand">Command type.</typeparam>
    /// <param name="entity">Entity instance.</param>
    /// <param name="command">Command to execute.</param>
    /// <param name="commandMetadata">Command metadata (optional, will be captured if null).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The entity after command handling.</returns>
    /// <exception cref="UnauthorizedAccessException">Thrown if authorization fails.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <see cref="DcbEntity.DecisionQuery"/> is null.</exception>
    Task<TEntity> HandleCommandAsync<TEntity, TCommand>(
        TEntity entity,
        TCommand command,
        CommandMetadata? commandMetadata = null,
        CancellationToken cancellationToken = default)
        where TEntity : DcbEntity
        where TCommand : ICommand;
}
