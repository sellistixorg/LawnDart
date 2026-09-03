using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Dcb;

/// <summary>
/// Repository for DCB-style entities that use tag-based consistency boundaries.
/// Provides an abstraction over the event store for DCB operations.
/// </summary>
public interface IDcbRepository
{
    /// <summary>
    /// Rebuilds state from events matching the given tags.
    /// </summary>
    /// <typeparam name="TState">State type to rebuild.</typeparam>
    /// <param name="tags">Tags to query events by.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Rebuilt state.</returns>
    Task<TState> GetStateAsync<TState>(
        string[] tags,
        CancellationToken cancellationToken = default) where TState : IState, new();

    /// <summary>
    /// Rebuilds state from events matching the given tags as of a specific point in time.
    /// Does not use snapshots to ensure correct temporal semantics.
    /// </summary>
    /// <typeparam name="TState">State type to rebuild.</typeparam>
    /// <param name="tags">Tags to query events by.</param>
    /// <param name="asOfTimestamp">Include only events with <see cref="EventMetadata.Timestamp"/> &lt;= this value (inclusive).</param>
    /// <param name="asOfSequencePosition">Include only events with SequencePosition &lt;= this value (inclusive).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Rebuilt state at the specified point in time.</returns>
    Task<TState> GetStateAtAsync<TState>(
        string[] tags,
        DateTime? asOfTimestamp = null,
        long? asOfSequencePosition = null,
        CancellationToken cancellationToken = default) where TState : IState, new();
    
    /// <summary>
    /// Gets the last sequence position for events matching the given tags.
    /// Used as a checkpoint for optimistic concurrency. Backed by
    /// <see cref="IEventStore.GetMaxSequencePositionAsync"/>.
    /// </summary>
    /// <param name="tags">Tags to query events by.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Last sequence position, or 0 if no events found.</returns>
    Task<long> GetLastSequencePositionAsync(
        string[] tags,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Appends events with automatic tag generation, metadata enrichment, and authorization.
    /// </summary>
    /// <param name="events">Events to append.</param>
    /// <param name="tags">Tags to apply to the events.</param>
    /// <param name="condition">Optional append condition for optimistic concurrency.</param>
    /// <param name="commandMetadata">Optional command metadata for enrichment and authorization.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Sequence positions assigned to the events.</returns>
    Task<IReadOnlyList<long>> AppendWithContextAsync(
        IEnumerable<IEvent> events,
        IEnumerable<string> tags,
        AppendCondition? condition = null,
        CommandMetadata? commandMetadata = null,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Creates a new DCB entity instance with the specified tags.
    /// </summary>
    /// <typeparam name="TEntity">Entity type.</typeparam>
    /// <param name="tags">Tags that identify this entity.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>New entity instance.</returns>
    Task<TEntity> CreateEntityAsync<TEntity>(
        string[] tags,
        CancellationToken cancellationToken = default) where TEntity : DcbEntity, new();
    
    /// <summary>
    /// Gets an existing entity or creates a new one if it doesn't exist.
    /// </summary>
    /// <typeparam name="TEntity">Entity type.</typeparam>
    /// <param name="tags">Tags that identify this entity.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Existing or new entity instance.</returns>
    Task<TEntity> GetOrCreateEntityAsync<TEntity>(
        string[] tags,
        CancellationToken cancellationToken = default) where TEntity : DcbEntity, new();
    
    /// <summary>
    /// Handles a command on a DCB entity with optional authorization checking.
    /// This method performs authorization (if enabled), executes the command, and persists events.
    /// If <paramref name="commandMetadata"/> is <c>null</c>, metadata is captured automatically
    /// from the registered <see cref="IMetadataProvider"/>.
    /// </summary>
    /// <typeparam name="TEntity">Entity type.</typeparam>
    /// <typeparam name="TCommand">Command type.</typeparam>
    /// <param name="entity">Entity instance.</param>
    /// <param name="command">Command to execute.</param>
    /// <param name="commandMetadata">Command metadata (optional, will be captured if null).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The entity after command handling.</returns>
    /// <exception cref="UnauthorizedAccessException">Thrown if authorization fails.</exception>
    Task<TEntity> HandleCommandAsync<TEntity, TCommand>(
        TEntity entity,
        TCommand command,
        CommandMetadata? commandMetadata = null,
        CancellationToken cancellationToken = default)
        where TEntity : DcbEntity
        where TCommand : ICommand;
}
