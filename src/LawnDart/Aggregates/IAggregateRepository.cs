using LawnDart.Metadata;

namespace LawnDart.Aggregates;

/// <summary>
/// Repository for loading and persisting aggregates.
/// </summary>
public interface IAggregateRepository
{
    /// <summary>
    /// Gets an aggregate by ID, loading events from the event store.
    /// </summary>
    /// <typeparam name="T">The aggregate type.</typeparam>
    /// <param name="id">The aggregate ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The loaded aggregate, or null if not found.</returns>
    Task<T?> GetAsync<T>(Guid id, CancellationToken cancellationToken = default) where T : AggregateRoot, new();

    /// <summary>
    /// Creates a new aggregate instance with the StreamId automatically set.
    /// This method does not check if the aggregate already exists - use GetOrCreateAsync for that.
    /// </summary>
    /// <typeparam name="T">The aggregate type.</typeparam>
    /// <param name="id">The aggregate ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A new aggregate instance with StreamId set.</returns>
    Task<T> CreateAsync<T>(Guid id, CancellationToken cancellationToken = default) where T : AggregateRoot, new();

    /// <summary>
    /// Gets an existing aggregate or creates a new one if it doesn't exist.
    /// This is the recommended method for handling commands, as it ensures the aggregate state
    /// is loaded from the event store before handling commands, preventing duplicate creation
    /// and ensuring proper version tracking.
    /// </summary>
    /// <typeparam name="T">The aggregate type.</typeparam>
    /// <param name="id">The aggregate ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The existing aggregate (with state loaded) or a new aggregate instance.</returns>
    Task<T> GetOrCreateAsync<T>(Guid id, CancellationToken cancellationToken = default) where T : AggregateRoot, new();

    /// <summary>
    /// Persists an aggregate's pending events to the event store.
    /// Handles versioning, metadata propagation, and outbox pattern.
    /// If StreamId is not set, attempts to auto-set it from aggregate state or events.
    /// </summary>
    /// <param name="aggregate">The aggregate to persist.</param>
    /// <param name="commandMetadata">Metadata from the command that caused these events.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    Task SaveAsync(AggregateRoot aggregate, CommandMetadata commandMetadata, CancellationToken cancellationToken = default);

    /// <summary>
    /// Handles a command on an aggregate with authorization checks.
    /// Performs authorization (if configured), calls HandleAsync on the aggregate,
    /// and persists events to the event store.
    /// </summary>
    /// <typeparam name="T">The aggregate type.</typeparam>
    /// <typeparam name="TCommand">The command type.</typeparam>
    /// <param name="aggregate">The aggregate instance.</param>
    /// <param name="command">The command to handle.</param>
    /// <param name="commandMetadata">Command metadata (optional, will be captured if null).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The aggregate after handling the command.</returns>
    Task<T> HandleCommandAsync<T, TCommand>(T aggregate, TCommand command, CommandMetadata? commandMetadata = null, CancellationToken cancellationToken = default)
        where T : AggregateRoot
        where TCommand : ICommand;
}

