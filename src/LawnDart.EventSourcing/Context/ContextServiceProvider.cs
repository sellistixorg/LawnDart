using Microsoft.Extensions.DependencyInjection;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventStore;
using LawnDart.Snapshots;

namespace LawnDart.EventSourcing.Context;

/// <summary>
/// An <see cref="IServiceProvider"/> wrapper that transparently redirects requests for
/// context-specific interfaces to their keyed counterparts in the root provider.
/// </summary>
/// <remarks>
/// <para>
/// Used as the service provider passed to <see cref="ActivatorUtilities.CreateInstance"/>
/// when instantiating command handlers and other domain services registered via
/// <c>WithCommandHandlers()</c>.  Because the handler's constructor receives
/// <c>IAggregateRepository</c> (unkeyed), the application code never needs to know
/// which bounded context it belongs to — this provider silently forwards the request to
/// the keyed version for <see cref="ContextName"/>.
/// </para>
/// <para>
/// Types intercepted: <see cref="IEventStore"/>, <see cref="IStreamRegistry"/>,
/// <see cref="IAggregateRepository"/>, <see cref="IDcbRepository"/>,
/// <see cref="ISnapshotStore"/>, <see cref="IDcbSnapshotStore"/>.
/// Additional types can be registered via <see cref="RegisterContextSpecificType"/>.
/// </para>
/// </remarks>
internal sealed class ContextServiceProvider : IServiceProvider, IKeyedServiceProvider
{
    private static readonly HashSet<Type> _contextSpecificTypes = new()
    {
        typeof(IEventStore),
        typeof(IStreamRegistry),
        typeof(IAggregateRepository),
        typeof(IDcbRepository),
        typeof(ISnapshotStore),
        typeof(IDcbSnapshotStore),
    };

    private readonly IServiceProvider _root;
    private readonly IKeyedServiceProvider? _keyedRoot;

    /// <summary>The context name used as the DI key when resolving context-specific types.</summary>
    internal string ContextName { get; }

    internal ContextServiceProvider(IServiceProvider root, string contextName)
    {
        _root       = root ?? throw new ArgumentNullException(nameof(root));
        _keyedRoot  = root as IKeyedServiceProvider;
        ContextName = contextName ?? throw new ArgumentNullException(nameof(contextName));
    }

    /// <summary>
    /// Allows projection infrastructure (and other packages) to add types that should be
    /// resolved as keyed services when this provider is active.
    /// </summary>
    /// <remarks>
    /// Call this from DI extension methods (e.g. <c>WithProjections()</c>) to ensure that
    /// types like <c>ICheckpointStore</c> are also context-aware.  This method is safe to
    /// call multiple times with the same type.
    /// </remarks>
    internal static void RegisterContextSpecificType(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        _contextSpecificTypes.Add(type);
    }

    /// <inheritdoc/>
    public object? GetService(Type serviceType)
    {
        if (_contextSpecificTypes.Contains(serviceType) && _keyedRoot is not null)
            return _keyedRoot.GetKeyedService(serviceType, ContextName);

        return _root.GetService(serviceType);
    }

    /// <inheritdoc/>
    public object? GetKeyedService(Type serviceType, object? serviceKey) =>
        _keyedRoot?.GetKeyedService(serviceType, serviceKey);

    /// <inheritdoc/>
    public object GetRequiredKeyedService(Type serviceType, object? serviceKey)
    {
        if (_keyedRoot is null)
            throw new InvalidOperationException(
                "The underlying IServiceProvider does not support keyed services.");

        return _keyedRoot.GetRequiredKeyedService(serviceType, serviceKey);
    }
}
