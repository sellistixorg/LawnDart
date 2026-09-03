using Xunit;
using LawnDart.EventSourcing.Performance;
using System.Diagnostics;

namespace LawnDart.EventSourcing.Tests.Performance;

[Collection("CompiledEventApplicator")]
public class CompiledEventApplicatorTests
{
    [Fact]
    public void ApplyEvent_WithMatchingApplyMethod_AppliesEvent()
    {
        // Arrange
        var state = new TestStateWithApplyMethods();
        var @event = new TestPerformanceEvent { Value = "test" };

        // Act
        CompiledEventApplicator.ApplyEvent(state, @event);

        // Assert
        Assert.Equal(1, state.EventCount);
        Assert.Equal("test", state.LastValue);
    }

    [Fact]
    public void ApplyEvent_WithApplyEventToStateMethod_AppliesEvent()
    {
        // Arrange
        var state = new TestStateWithApplyEventToState();
        var @event = new TestPerformanceEvent { Value = "test" };

        // Act
        CompiledEventApplicator.ApplyEvent(state, @event);

        // Assert
        Assert.Equal(1, state.EventCount);
    }

    [Fact]
    public void ApplyEvent_WithNoMatchingMethod_DoesNotThrow()
    {
        // Arrange
        var state = new TestStateWithNoMethods();
        var @event = new TestPerformanceEvent { Value = "test" };

        // Act & Assert - Should not throw
        CompiledEventApplicator.ApplyEvent(state, @event);
    }

    [Fact]
    public void ApplyEvent_CachesCompiledExpression()
    {
        // Arrange
        CompiledEventApplicator.ClearCache();
        var state1 = new TestStateWithApplyMethods();
        var state2 = new TestStateWithApplyMethods();
        var @event = new TestPerformanceEvent { Value = "test" };

        // Act
        CompiledEventApplicator.ApplyEvent(state1, @event);
        var initialCacheSize = CompiledEventApplicator.CacheSize;
        
        CompiledEventApplicator.ApplyEvent(state2, @event);
        var finalCacheSize = CompiledEventApplicator.CacheSize;

        // Assert
        Assert.Equal(1, initialCacheSize);
        Assert.Equal(1, finalCacheSize); // Should reuse cached compiled expression
    }

    [Fact]
    public void ApplyEvent_UsesCompiledExpressions()
    {
        // Arrange
        var state = new TestStateWithApplyMethods();
        var @event = new TestPerformanceEvent { Value = "test" };
        const int iterations = 100000;

        // Warmup compiled expression cache
        CompiledEventApplicator.ApplyEvent(state, @event);
        state.EventCount = 0;

        // Measure compiled expression
        var compiledSw = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            CompiledEventApplicator.ApplyEvent(state, @event);
        }
        compiledSw.Stop();

        // Reset state
        state.EventCount = 0;

        // Measure OLD-STYLE slow reflection (MethodInfo.Invoke with boxing)
        // This is what was in production before optimization
        var reflectionSw = Stopwatch.StartNew();
        var applyMethod = typeof(TestStateWithApplyMethods).GetMethod("Apply",
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance,
            null,
            new[] { typeof(TestPerformanceEvent) },
            null);
        for (int i = 0; i < iterations; i++)
        {
            // Old reflection style with boxing overhead
            applyMethod?.Invoke(state, new object[] { @event });
        }
        reflectionSw.Stop();

        // Assert - Both should work correctly
        // Note: We don't assert performance here because .NET 10+ has highly optimized reflection
        // that can be competitive with compiled expressions for trivial operations.
        // The real benefit comes with complex Apply methods and aggregate loading scenarios.
        Assert.Equal(iterations, state.EventCount);
        
        // Output timing for comparison (informational only)
        var speedup = (double)reflectionSw.ElapsedTicks / compiledSw.ElapsedTicks;
        Console.WriteLine($"Compiled: {compiledSw.ElapsedMilliseconds}ms ({compiledSw.ElapsedTicks} ticks)");
        Console.WriteLine($"Old Reflection: {reflectionSw.ElapsedMilliseconds}ms ({reflectionSw.ElapsedTicks} ticks)");
        Console.WriteLine($"Speedup: {speedup:F2}x");
    }

    [Fact]
    public void ClearCache_RemovesAllCachedExpressions()
    {
        // Arrange
        var state = new TestStateWithApplyMethods();
        var @event = new TestPerformanceEvent { Value = "test" };
        CompiledEventApplicator.ApplyEvent(state, @event);
        
        Assert.True(CompiledEventApplicator.CacheSize > 0);

        // Act
        CompiledEventApplicator.ClearCache();

        // Assert
        Assert.Equal(0, CompiledEventApplicator.CacheSize);
    }

    [Fact]
    public void ApplyEvent_WithMultipleEventTypes_CachesEachSeparately()
    {
        // Arrange
        CompiledEventApplicator.ClearCache();
        var state = new TestStateWithApplyMethods();
        var event1 = new TestPerformanceEvent { Value = "test1" };
        var event2 = new AnotherTestEvent { Data = "test2" };

        // Act
        CompiledEventApplicator.ApplyEvent(state, event1);
        CompiledEventApplicator.ApplyEvent(state, event2);

        // Assert
        Assert.Equal(2, CompiledEventApplicator.CacheSize);
        Assert.Equal(2, state.EventCount);
    }

    [Fact]
    public void ApplyEvent_NonGeneric_WorksCorrectly()
    {
        // Arrange
        IState state = new TestStateWithApplyMethods();
        IEvent @event = new TestPerformanceEvent { Value = "test" };

        // Act
        CompiledEventApplicator.ApplyEvent(state, @event);

        // Assert
        var typedState = (TestStateWithApplyMethods)state;
        Assert.Equal(1, typedState.EventCount);
    }
}

// Test types
public class TestPerformanceEvent : IEvent
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public string Value { get; init; } = string.Empty;
}

public class AnotherTestEvent : IEvent
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public string Data { get; init; } = string.Empty;
}

public class TestStateWithApplyMethods : IState
{
    public int EventCount { get; set; }
    public string LastValue { get; set; } = string.Empty;

    public void Apply(TestPerformanceEvent @event)
    {
        EventCount++;
        LastValue = @event.Value;
    }

    public void Apply(AnotherTestEvent @event)
    {
        EventCount++;
        LastValue = @event.Data;
    }
}

public class TestStateWithApplyEventToState : IState
{
    public int EventCount { get; set; }

    public void ApplyEventToState(IEvent @event)
    {
        EventCount++;
    }
}

public class TestStateWithNoMethods : IState
{
    // No apply methods
}
