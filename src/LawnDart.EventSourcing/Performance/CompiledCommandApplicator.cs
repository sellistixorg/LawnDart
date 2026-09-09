using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using LawnDart.Aggregates;
using LawnDart.Dcb;

namespace LawnDart.EventSourcing.Performance;

/// <summary>
/// Compiles and caches closed <c>Handle(TCommand)</c> / <c>Handle(TCommand, CancellationToken)</c>
/// methods on aggregates and DCB entities.
/// </summary>
public static class CompiledCommandApplicator
{
    private static readonly ConcurrentDictionary<CacheKey, Func<object, ICommand, CancellationToken, Task>?> HandlerCache = new();
    private static readonly ConcurrentDictionary<Type, bool> OverrideCache = new();

    private readonly struct CacheKey : IEquatable<CacheKey>
    {
        public readonly Type TargetType;
        public readonly Type CommandType;

        public CacheKey(Type targetType, Type commandType)
        {
            TargetType = targetType;
            CommandType = commandType;
        }

        public bool Equals(CacheKey other) =>
            TargetType == other.TargetType && CommandType == other.CommandType;

        public override bool Equals(object? obj) =>
            obj is CacheKey other && Equals(other);

        public override int GetHashCode() =>
            HashCode.Combine(TargetType, CommandType);
    }

    /// <summary>
    /// True when <paramref name="targetType"/> (or an intermediate base) overrides
    /// open-generic <c>HandleAsync&lt;TCommand&gt;</c>. Those types keep the generic path.
    /// </summary>
    public static bool OverridesHandleAsync(Type targetType)
    {
        ArgumentNullException.ThrowIfNull(targetType);
        return OverrideCache.GetOrAdd(targetType, DetectOverride);
    }

    /// <summary>
    /// Invokes the closed <c>Handle</c> for <paramref name="command"/> on <paramref name="target"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">No matching <c>Handle</c> method exists.</exception>
    public static Task DispatchAsync(object target, ICommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(command);

        var targetType = target.GetType();
        var commandType = command.GetType();
        var key = new CacheKey(targetType, commandType);
        var handler = HandlerCache.GetOrAdd(key, static k => CompileHandler(k.TargetType, k.CommandType));
        if (handler is null)
        {
            throw new InvalidOperationException(
                $"{targetType.Name} does not declare Handle({commandType.Name}) or " +
                $"Handle({commandType.Name}, CancellationToken).");
        }

        return handler(target, command, cancellationToken);
    }

    /// <summary>
    /// Clears compiled delegates. Useful for tests.
    /// </summary>
    public static void ClearCache()
    {
        HandlerCache.Clear();
        OverrideCache.Clear();
    }

    /// <summary>
    /// Number of compiled (target, command) entries, including cached misses.
    /// </summary>
    public static int CacheSize => HandlerCache.Count;

    private static bool DetectOverride(Type targetType)
    {
        for (var type = targetType; type != null && type != typeof(object); type = type.BaseType)
        {
            if (IsFrameworkCommandHost(type))
                return false;

            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                if (method.Name != nameof(AggregateRoot.HandleAsync) || !method.IsGenericMethodDefinition)
                    continue;

                if (method.GetParameters().Length >= 1)
                    return true;
            }
        }

        return false;
    }

    private static bool IsFrameworkCommandHost(Type type)
    {
        if (type == typeof(AggregateRoot) || type == typeof(DcbEntity))
            return true;

        if (!type.IsGenericType)
            return false;

        var definition = type.GetGenericTypeDefinition();
        return definition == typeof(AggregateRoot<>) || definition == typeof(DcbEntity<>);
    }

    private static Func<object, ICommand, CancellationToken, Task>? CompileHandler(Type targetType, Type commandType)
    {
        var withCancellation = FindHandle(targetType, commandType, includeCancellation: true);
        if (withCancellation != null)
            return Compile(targetType, commandType, withCancellation, hasCancellation: true);

        var withoutCancellation = FindHandle(targetType, commandType, includeCancellation: false);
        if (withoutCancellation != null)
            return Compile(targetType, commandType, withoutCancellation, hasCancellation: false);

        return null;
    }

    private static MethodInfo? FindHandle(Type targetType, Type commandType, bool includeCancellation)
    {
        var parameterTypes = includeCancellation
            ? new[] { commandType, typeof(CancellationToken) }
            : new[] { commandType };

        return targetType.GetMethod(
            "Handle",
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            types: parameterTypes,
            modifiers: null);
    }

    private static Func<object, ICommand, CancellationToken, Task> Compile(
        Type targetType,
        Type commandType,
        MethodInfo method,
        bool hasCancellation)
    {
        var targetParam = Expression.Parameter(typeof(object), "target");
        var commandParam = Expression.Parameter(typeof(ICommand), "command");
        var cancellationParam = Expression.Parameter(typeof(CancellationToken), "cancellationToken");

        var instance = Expression.Convert(targetParam, targetType);
        var typedCommand = Expression.Convert(commandParam, commandType);

        Expression call = hasCancellation
            ? Expression.Call(instance, method, typedCommand, cancellationParam)
            : Expression.Call(instance, method, typedCommand);

        var returnType = method.ReturnType;
        Expression body;
        if (returnType == typeof(void))
        {
            body = Expression.Block(call, Expression.Constant(Task.CompletedTask, typeof(Task)));
        }
        else if (returnType == typeof(Task) ||
                 (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(Task<>)))
        {
            body = Expression.Convert(call, typeof(Task));
        }
        else if (returnType == typeof(ValueTask))
        {
            body = Expression.Call(call, nameof(ValueTask.AsTask), typeArguments: null);
        }
        else if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(ValueTask<>))
        {
            body = Expression.Convert(
                Expression.Call(call, nameof(ValueTask.AsTask), typeArguments: null),
                typeof(Task));
        }
        else
        {
            throw new InvalidOperationException(
                $"{method.DeclaringType?.Name}.{method.Name} must return void, Task, or ValueTask.");
        }

        return Expression.Lambda<Func<object, ICommand, CancellationToken, Task>>(
            body, targetParam, commandParam, cancellationParam).Compile();
    }
}
