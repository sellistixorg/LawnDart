namespace LawnDart.EventStore;

/// <summary>
/// Read-only registry of all bounded context names registered in the application.
/// </summary>
public interface IBoundedContextRegistry
{
    /// <summary>Returns all registered context names in registration order.</summary>
    IReadOnlyList<string> ContextNames { get; }

    /// <summary>Returns <see langword="true"/> when more than one context is registered.</summary>
    bool IsMultiContext { get; }

    /// <summary>Returns <see langword="true"/> when the given name is registered.</summary>
    bool Contains(string contextName);
}

/// <summary>
/// Mutable registry used during host startup to track bounded context names.
/// Exposed as <see cref="IBoundedContextRegistry"/> to application code.
/// </summary>
public sealed class BoundedContextRegistry : IBoundedContextRegistry
{
    private readonly List<string> _names = new();

    /// <inheritdoc/>
    public IReadOnlyList<string> ContextNames => _names.AsReadOnly();

    /// <inheritdoc/>
    public bool IsMultiContext => _names.Count > 1;

    /// <inheritdoc/>
    public bool Contains(string contextName) =>
        _names.Contains(contextName, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Registers a context name, enforcing uniqueness.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the name is already registered.</exception>
    public void Register(string contextName)
    {
        if (Contains(contextName))
            throw new InvalidOperationException(
                $"A bounded context with name '{contextName}' has already been registered. " +
                "Each context name must be unique within a single application.");

        _names.Add(contextName);
    }
}
