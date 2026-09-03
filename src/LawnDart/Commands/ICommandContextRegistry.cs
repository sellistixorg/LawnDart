namespace LawnDart;

/// <summary>
/// Maps command types to the bounded context name that owns their handler.
/// </summary>
/// <remarks>
/// Populated automatically by <c>WithCommandHandlers()</c> on the
/// <c>BoundedContextBuilder</c>.  The <c>ContextAwareCommandDispatcher</c> queries
/// this registry at dispatch time to route each command to the correct keyed handler.
/// </remarks>
public interface ICommandContextRegistry
{
    /// <summary>
    /// Returns the context name associated with <paramref name="commandType"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <paramref name="commandType"/> has not been registered.
    /// </exception>
    string GetContextName(Type commandType);

    /// <summary>
    /// Associates <paramref name="commandType"/> with <paramref name="contextName"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the type is already mapped to a <em>different</em> context.
    /// </exception>
    void Register(Type commandType, string contextName);

    /// <summary>
    /// Returns a snapshot of all registered mappings, grouped by context name.
    /// Intended for diagnostics and the startup validator.
    /// </summary>
    IReadOnlyDictionary<string, IReadOnlyList<Type>> GetAllRegistrations();

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="commandType"/> has been
    /// registered with a context.
    /// </summary>
    bool IsRegistered(Type commandType);
}
