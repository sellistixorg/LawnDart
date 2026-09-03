using System.Linq.Expressions;
using LawnDart;

namespace LawnDart.Projections.Sdk;

/// <summary>
/// Defines the scope and partitioning strategy for a projection.
/// </summary>
public enum ProjectionScope
{
    /// <summary>
    /// Per-stream projection - partitioned by StreamId hash across nodes.
    /// Each node processes a subset of streams. Scales horizontally.
    /// </summary>
    PerStream,
    
    /// <summary>
    /// Global projection - processes all events on designated node (typically node 0).
    /// Used for cross-stream aggregations and indexes. Does not partition.
    /// </summary>
    Global
}

/// <summary>
/// Controls how projection view writes are acknowledged.
/// </summary>
public enum ProjectionWriteMode
{
    /// <summary>
    /// Acknowledge after the write is queued in the write pipeline.
    /// Higher throughput, but checkpoint/view durability may lag briefly.
    /// </summary>
    Buffered,

    /// <summary>
    /// Acknowledge only after durable persistence completes.
    /// Lower throughput, strongest correctness semantics.
    /// </summary>
    Strict
}

/// <summary>
/// Metadata describing a projection type and its configuration
/// </summary>
public record ProjectionDescriptor
{
    /// <summary>
    /// Unique identifier for the projection type (e.g., "OrderSummary", "CartTotals")
    /// </summary>
    public required string ProjectionType { get; init; }

    /// <summary>
    /// The .NET type of the projection handler
    /// </summary>
    public required Type HandlerType { get; init; }

    /// <summary>
    /// The .NET type of the view/read model
    /// </summary>
    public required Type ViewType { get; init; }

    /// <summary>
    /// Stream types this projection is interested in (e.g., "Order", "Cart")
    /// If empty, processes all streams.
    /// </summary>
    public List<string> StreamTypes { get; init; } = new();

    /// <summary>
    /// Projection scope - determines partitioning strategy.
    /// PerStream (default): Partitioned by StreamId hash across nodes for horizontal scaling.
    /// Global: Runs on single node (node 0) for cross-stream aggregations.
    /// </summary>
    public ProjectionScope Scope { get; init; } = ProjectionScope.PerStream;

    /// <summary>
    /// Version of the projection (for blue-green deployments)
    /// </summary>
    public int Version { get; init; } = 1;

    /// <summary>
    /// Number of projection instances to run in parallel (for partitioning)
    /// </summary>
    public int InstanceCount { get; init; } = 1;

    /// <summary>
    /// Whether to store views in Redis for fast reads
    /// </summary>
    public bool UseRedis { get; init; } = true;

    /// <summary>
    /// Whether to store views in SQL for reliable persistence
    /// </summary>
    public bool UseSql { get; init; } = true;

    /// <summary>
    /// Checkpoint save interval (events)
    /// </summary>
    public int CheckpointInterval { get; init; } = 100;

    /// <summary>
    /// Checkpoint save interval (time)
    /// </summary>
    public TimeSpan CheckpointTimeInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// How frequently the coordinator polls the event store for new events.
    /// Applies only to poll mode; stores driving projections by push ignore this.
    /// Decrease for lower-latency projections; increase for background / batch projections.
    /// Default: 100 ms
    /// </summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Controls whether checkpoint/view updates are acknowledged on queueing (Buffered)
    /// or only after durable persistence confirmation (Strict).
    /// Default: Strict.
    /// </summary>
    public ProjectionWriteMode WriteMode { get; init; } = ProjectionWriteMode.Strict;

    /// <summary>
    /// Compiled event handlers for fast invocation (no reflection overhead).
    /// Key: Event type, Value: Compiled delegate (projection, event) => void
    /// This is built once at startup and reused for all events.
    /// </summary>
    private Dictionary<Type, Action<object, IEvent>>? _compiledHandlers;

    /// <summary>
    /// Compiled ProcessEvent method for fast invocation.
    /// </summary>
    private Action<object, IEvent, long>? _compiledProcessEvent;

    /// <summary>
    /// Compiled GetView method for fast invocation.
    /// </summary>
    private Func<object, object>? _compiledGetView;

    /// <summary>
    /// Gets or initializes the compiled event handlers.
    /// This compiles all Handle methods at startup for maximum performance.
    /// </summary>
    public Dictionary<Type, Action<object, IEvent>> GetCompiledHandlers()
    {
        if (_compiledHandlers != null)
            return _compiledHandlers;

        _compiledHandlers = new Dictionary<Type, Action<object, IEvent>>();

        // Find all Handle methods on the handler type
        var handleMethods = HandlerType.GetMethods()
            .Where(m => m.Name == "Handle" && m.GetParameters().Length == 1);

        foreach (var method in handleMethods)
        {
            var eventType = method.GetParameters()[0].ParameterType;
            
            // Create compiled delegate: (projection, event) => projection.Handle((TEvent)event)
            var projectionParam = Expression.Parameter(typeof(object), "projection");
            var eventParam = Expression.Parameter(typeof(IEvent), "event");

            var call = Expression.Call(
                Expression.Convert(projectionParam, HandlerType),
                method,
                Expression.Convert(eventParam, eventType));

            var lambda = Expression.Lambda<Action<object, IEvent>>(call, projectionParam, eventParam);
            var compiled = lambda.Compile();

            _compiledHandlers[eventType] = compiled;
        }

        return _compiledHandlers;
    }

    /// <summary>
    /// Gets the compiled ProcessEvent method.
    /// </summary>
    public Action<object, IEvent, long> GetCompiledProcessEvent()
    {
        if (_compiledProcessEvent != null)
            return _compiledProcessEvent;

        var method = HandlerType.GetMethod("ProcessEvent");
        if (method == null)
            throw new InvalidOperationException($"ProcessEvent method not found on {HandlerType.Name}");

        // Create compiled delegate: (projection, event, position) => projection.ProcessEvent(event, position)
        var projectionParam = Expression.Parameter(typeof(object), "projection");
        var eventParam = Expression.Parameter(typeof(IEvent), "event");
        var positionParam = Expression.Parameter(typeof(long), "position");

        var call = Expression.Call(
            Expression.Convert(projectionParam, HandlerType),
            method,
            eventParam,
            positionParam);

        var lambda = Expression.Lambda<Action<object, IEvent, long>>(call, projectionParam, eventParam, positionParam);
        _compiledProcessEvent = lambda.Compile();

        return _compiledProcessEvent;
    }

    /// <summary>
    /// Gets the compiled GetView method.
    /// </summary>
    public Func<object, object> GetCompiledGetView()
    {
        if (_compiledGetView != null)
            return _compiledGetView;

        var method = HandlerType.GetMethod("GetView");
        if (method == null)
            throw new InvalidOperationException($"GetView method not found on {HandlerType.Name}");

        // Create compiled delegate: (projection) => projection.GetView()
        var projectionParam = Expression.Parameter(typeof(object), "projection");

        var call = Expression.Call(
            Expression.Convert(projectionParam, HandlerType),
            method);

        var lambda = Expression.Lambda<Func<object, object>>(call, projectionParam);
        _compiledGetView = lambda.Compile();

        return _compiledGetView;
    }
}
