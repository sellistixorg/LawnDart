namespace LawnDart.AspNetCore;

/// <summary>
/// Controls how command types are grouped into OpenAPI tags.
/// </summary>
public enum CommandTagStrategy
{
    /// <summary>
    /// Derive the tag from the command's namespace (e.g., <c>Acme.Orders.Commands.CreateOrderCommand</c> → tag <c>Orders</c>).
    /// </summary>
    ByNamespace,

    /// <summary>
    /// All commands share a single <c>Commands</c> tag regardless of namespace.
    /// </summary>
    Flat
}

/// <summary>
/// Configuration options for auto-registered HTTP command endpoints.
/// </summary>
public sealed class HttpCommandOptions
{
    /// <summary>
    /// The route prefix applied to all command endpoints.
    /// Default: <c>api</c>, yielding routes like <c>/api/orders/create-order</c>.
    /// </summary>
    public string RoutePrefix { get; set; } = "api";

    /// <summary>
    /// Strategy used to assign OpenAPI tags to command endpoints.
    /// Default: <see cref="CommandTagStrategy.ByNamespace"/>.
    /// </summary>
    public CommandTagStrategy TagStrategy { get; set; } = CommandTagStrategy.ByNamespace;

    /// <summary>
    /// The fallback OpenAPI tag used when <see cref="TagStrategy"/> is <see cref="CommandTagStrategy.Flat"/>
    /// or when a namespace-based tag cannot be derived.
    /// Default: <c>Commands</c>.
    /// </summary>
    public string FallbackTag { get; set; } = "Commands";
}
