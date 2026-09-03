using System.Collections.Concurrent;

namespace LawnDart;

/// <summary>
/// Default thread-safe implementation of <see cref="ICommandContextRegistry"/>.
/// </summary>
/// <remarks>
/// A single instance is shared across all calls to <c>WithCommandHandlers()</c> during
/// host startup.  After the host is built the registry becomes effectively immutable —
/// the dispatcher only calls <see cref="GetContextName"/> at runtime.
/// </remarks>
public sealed class DefaultCommandContextRegistry : ICommandContextRegistry
{
    private readonly ConcurrentDictionary<Type, string> _commandToContext = new();

    /// <inheritdoc/>
    public string GetContextName(Type commandType)
    {
        ArgumentNullException.ThrowIfNull(commandType);

        if (_commandToContext.TryGetValue(commandType, out var contextName))
            return contextName;

        throw new InvalidOperationException(
            $"Command type '{commandType.FullName}' has not been registered with any bounded context. " +
            "Ensure WithCommandHandlers() is called on the BoundedContextBuilder for the context that owns this command's handler.");
    }

    /// <inheritdoc/>
    public void Register(Type commandType, string contextName)
    {
        ArgumentNullException.ThrowIfNull(commandType);
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", nameof(contextName));

        if (_commandToContext.TryGetValue(commandType, out var existing) &&
            !string.Equals(existing, contextName, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Command type '{commandType.FullName}' is already registered with context '{existing}'. " +
                $"A command handler must belong to exactly one bounded context (attempted: '{contextName}').");
        }

        _commandToContext[commandType] = contextName;
    }

    /// <inheritdoc/>
    public bool IsRegistered(Type commandType) =>
        commandType is not null && _commandToContext.ContainsKey(commandType);

    /// <inheritdoc/>
    public IReadOnlyDictionary<string, IReadOnlyList<Type>> GetAllRegistrations()
    {
        var result = new Dictionary<string, List<Type>>(StringComparer.OrdinalIgnoreCase);

        foreach (var (type, context) in _commandToContext)
        {
            if (!result.TryGetValue(context, out var list))
            {
                list = new List<Type>();
                result[context] = list;
            }
            list.Add(type);
        }

        return result.ToDictionary(
            kvp => kvp.Key,
            kvp => (IReadOnlyList<Type>)kvp.Value.AsReadOnly(),
            StringComparer.OrdinalIgnoreCase);
    }
}
