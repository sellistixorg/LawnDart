namespace LawnDart.Projections.Lightweight;

/// <summary>
/// Marks a <see cref="LawnDart.Projections.Sdk.ProjectionBase{TView}"/> subclass as a
/// multi-stream projection that builds one view instance per logical entity across multiple
/// aggregate stream types.
/// </summary>
/// <remarks>
/// <para>
/// A <c>MultiStreamProjection</c> is ideal when a single read model must combine events from more
/// than one stream type — for example, an order fulfilment view that consumes events from both
/// <c>OrderAggregate</c> and <c>ShipmentAggregate</c> streams.
/// </para>
/// <para>
/// The handler class <strong>must also implement</strong> <see cref="IMultiStreamEntityResolver"/>.
/// The resolver's <see cref="IMultiStreamEntityResolver.GetEntityId"/> method is called for every
/// matching event to determine which view instance should receive it.
/// </para>
/// <para>
/// Events are read from the store using a DCB-style query filtered to the types listed in
/// <see cref="EventTypes"/>, making this projection efficient even against a high-volume global
/// sequence.
/// </para>
/// <example>
/// <code>
/// [MultiStreamProjection("OrderFulfillment",
///     typeof(OrderPlaced), typeof(OrderShipped))]
/// [ProjectionEndpoint("/api/views/fulfillment/{orderId}", requiredPermission: null)]
/// public class OrderFulfillmentProjection
///     : ProjectionBase&lt;OrderFulfillmentView&gt;, IMultiStreamEntityResolver
/// {
///     public string? GetEntityId(IEvent e) => e switch
///     {
///         OrderPlaced  op => op.OrderId.ToString(),
///         OrderShipped os => os.OrderId.ToString(),
///         _               => null
///     };
///
///     public void Handle(OrderPlaced e)  { State.OrderId = e.OrderId; ... }
///     public void Handle(OrderShipped e) { State.ShippedAt = e.Timestamp; ... }
/// }
/// </code>
/// </example>
/// </remarks>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class MultiStreamProjectionAttribute : ProjectionDefinitionAttribute
{
    /// <summary>
    /// Gets the CLR event types this projection subscribes to. The runner builds a DCB query from
    /// each CLR type's simple name (<see cref="P:System.Type.Name"/>). An empty array causes
    /// <see cref="LawnDart.EventStore.Query.All()"/> to be used instead.
    /// </summary>
    public Type[] EventTypes { get; }

    /// <summary>
    /// Initializes a new <see cref="MultiStreamProjectionAttribute"/> with
    /// <see cref="TenantScope.TenantScoped"/>.
    /// </summary>
    /// <param name="name">Unique projection name.</param>
    /// <param name="eventTypes">
    /// Zero or more CLR event types to subscribe to. Pass an empty array to subscribe to all events.
    /// </param>
    public MultiStreamProjectionAttribute(string name, params Type[] eventTypes)
        : base(name, TenantScope.TenantScoped)
    {
        EventTypes = eventTypes ?? Array.Empty<Type>();
    }

    /// <summary>
    /// Initializes a new <see cref="MultiStreamProjectionAttribute"/> with an explicit tenant scope.
    /// </summary>
    /// <param name="name">Unique projection name.</param>
    /// <param name="tenantScope">Tenant isolation mode.</param>
    /// <param name="eventTypes">Zero or more CLR event types to subscribe to.</param>
    public MultiStreamProjectionAttribute(string name, TenantScope tenantScope, params Type[] eventTypes)
        : base(name, tenantScope)
    {
        EventTypes = eventTypes ?? Array.Empty<Type>();
    }
}
