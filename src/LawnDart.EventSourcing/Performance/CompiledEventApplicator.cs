using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace LawnDart.EventSourcing.Performance;

/// <summary>
/// Provides compiled expressions for fast event application to state.
/// Replaces slow reflection-based invocation with compiled delegates.
/// Uses a custom struct key to minimize allocations.
/// </summary>
public static class CompiledEventApplicator
{
    // Use custom struct key instead of tuple to avoid allocations
    private static readonly ConcurrentDictionary<CacheKey, Action<object, IEvent>> Cache = new();
    
    /// <summary>
    /// Cache key struct to minimize allocations during lookups.
    /// </summary>
    private readonly struct CacheKey : IEquatable<CacheKey>
    {
        public readonly Type StateType;
        public readonly Type EventType;
        
        public CacheKey(Type stateType, Type eventType)
        {
            StateType = stateType;
            EventType = eventType;
        }
        
        public bool Equals(CacheKey other) => 
            StateType == other.StateType && EventType == other.EventType;
        
        public override bool Equals(object? obj) => 
            obj is CacheKey other && Equals(other);
        
        public override int GetHashCode() => 
            HashCode.Combine(StateType, EventType);
    }
    
    /// <summary>
    /// Applies an event to a state object using a compiled expression.
    /// The compiled expression is cached for reuse.
    /// </summary>
    /// <typeparam name="TState">State type.</typeparam>
    /// <param name="state">State instance.</param>
    /// <param name="event">Event to apply.</param>
    public static void ApplyEvent<TState>(TState state, IEvent @event) where TState : IState
    {
        if (state == null) throw new ArgumentNullException(nameof(state));
        if (@event == null) throw new ArgumentNullException(nameof(@event));
        
        var key = new CacheKey(typeof(TState), @event.GetType());
        
        // Get or create compiled applicator
        var applicator = Cache.GetOrAdd(key, k => CompileApplicator(k.StateType, k.EventType));
        
        // Apply event
        applicator(state, @event);
    }
    
    /// <summary>
    /// Applies an event to a state object (non-generic version).
    /// </summary>
    /// <param name="state">State instance.</param>
    /// <param name="event">Event to apply.</param>
    public static void ApplyEvent(IState state, IEvent @event)
    {
        if (state == null) throw new ArgumentNullException(nameof(state));
        if (@event == null) throw new ArgumentNullException(nameof(@event));
        
        var key = new CacheKey(state.GetType(), @event.GetType());
        
        // Get or create compiled applicator
        var applicator = Cache.GetOrAdd(key, k => CompileApplicator(k.StateType, k.EventType));
        
        // Apply event
        applicator(state, @event);
    }
    
    /// <summary>
    /// Compiles an expression that calls the Apply method on a state for a specific event type.
    /// Pattern: (state, @event) => state.Apply((TEvent)@event)
    /// </summary>
    private static Action<object, IEvent> CompileApplicator(Type stateType, Type eventType)
    {
        // Look for an Apply method that accepts the specific event type
        var applyMethod = stateType.GetMethod(
            "Apply",
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            new[] { eventType },
            null);
        
        if (applyMethod != null)
        {
            // Create expression: (object state, IEvent @event) => ((TState)state).Apply((TEvent)@event)
            var stateParam = Expression.Parameter(typeof(object), "state");
            var eventParam = Expression.Parameter(typeof(IEvent), "event");
            
            var stateConverted = Expression.Convert(stateParam, stateType);
            var eventConverted = Expression.Convert(eventParam, eventType);
            
            var methodCall = Expression.Call(stateConverted, applyMethod, eventConverted);
            
            var lambda = Expression.Lambda<Action<object, IEvent>>(methodCall, stateParam, eventParam);
            return lambda.Compile();
        }
        
        // Fallback: look for ApplyEventToState method
        var applyEventToStateMethod = stateType.GetMethod(
            "ApplyEventToState",
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            new[] { typeof(IEvent) },
            null);
        
        if (applyEventToStateMethod != null)
        {
            // Create expression: (object state, IEvent @event) => ((TState)state).ApplyEventToState(@event)
            var stateParam = Expression.Parameter(typeof(object), "state");
            var eventParam = Expression.Parameter(typeof(IEvent), "event");
            
            var stateConverted = Expression.Convert(stateParam, stateType);
            
            var methodCall = Expression.Call(stateConverted, applyEventToStateMethod, eventParam);
            
            var lambda = Expression.Lambda<Action<object, IEvent>>(methodCall, stateParam, eventParam);
            return lambda.Compile();
        }
        
        // No applicable method found - return no-op
        return (state, @event) => { };
    }
    
    /// <summary>
    /// Clears the cache. Useful for testing or when types are dynamically loaded/unloaded.
    /// </summary>
    public static void ClearCache()
    {
        Cache.Clear();
    }
    
    /// <summary>
    /// Gets the current cache size.
    /// </summary>
    public static int CacheSize => Cache.Count;
}
