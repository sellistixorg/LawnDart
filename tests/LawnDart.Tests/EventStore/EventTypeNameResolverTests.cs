using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public class EventTypeNameResolverTests
{
    // ── Test types ────────────────────────────────────────────────────────────

    private record EventWithoutAttribute : LawnDart.IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    }

    [EventTypeName("my-stable-alias")]
    private record EventWithAlias : LawnDart.IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    }

    [EventTypeName("parent-alias")]
    private record ParentEvent : LawnDart.IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    }

    // Inherits from ParentEvent — attribute is NOT inherited (Inherited = false)
    private record ChildEvent : ParentEvent;

    // ── GetName — no attribute ────────────────────────────────────────────────

    [Fact]
    public void GetName_WithoutAttribute_ReturnsFullName()
    {
        var name = EventTypeNameResolver.GetName(typeof(EventWithoutAttribute));

        Assert.Equal(typeof(EventWithoutAttribute).FullName, name);
    }

    [Fact]
    public void GetName_WithoutAttribute_DoesNotReturnShortName()
    {
        var name = EventTypeNameResolver.GetName(typeof(EventWithoutAttribute));

        Assert.NotEqual(typeof(EventWithoutAttribute).Name, name);
    }

    // ── GetName — with attribute ──────────────────────────────────────────────

    [Fact]
    public void GetName_WithAttribute_ReturnsAttributeValue()
    {
        var name = EventTypeNameResolver.GetName(typeof(EventWithAlias));

        Assert.Equal("my-stable-alias", name);
    }

    [Fact]
    public void GetName_WithAttribute_DoesNotReturnFullName()
    {
        var name = EventTypeNameResolver.GetName(typeof(EventWithAlias));

        Assert.NotEqual(typeof(EventWithAlias).FullName, name);
    }

    // ── Attribute inheritance ─────────────────────────────────────────────────

    [Fact]
    public void GetName_ChildOfAttributedParent_ReturnsOwnFullName_NotParentAlias()
    {
        // The attribute has Inherited = false, so ChildEvent should NOT pick up "parent-alias"
        var name = EventTypeNameResolver.GetName(typeof(ChildEvent));

        Assert.Equal(typeof(ChildEvent).FullName, name);
        Assert.NotEqual("parent-alias", name);
    }

    // ── Caching ───────────────────────────────────────────────────────────────

    [Fact]
    public void GetName_CalledMultipleTimes_ReturnsSameStringReference()
    {
        // Same string instance from cache, not new allocation each time.
        var name1 = EventTypeNameResolver.GetName(typeof(EventWithAlias));
        var name2 = EventTypeNameResolver.GetName(typeof(EventWithAlias));

        Assert.Same(name1, name2);
    }

    [Fact]
    public void GetName_CalledMultipleTimes_ReturnsSameValue()
    {
        var name1 = EventTypeNameResolver.GetName(typeof(EventWithoutAttribute));
        var name2 = EventTypeNameResolver.GetName(typeof(EventWithoutAttribute));

        Assert.Equal(name1, name2);
    }

    // ── Warmup ────────────────────────────────────────────────────────────────

    [Fact]
    public void Warmup_ThenGetName_ReturnsSameResultAsWithoutWarmup()
    {
        // Warmup should not change the resolved value — only pre-populate cache
        var types = new[] { typeof(EventWithoutAttribute), typeof(EventWithAlias) };
        EventTypeNameResolver.Warmup(types);

        Assert.Equal(typeof(EventWithoutAttribute).FullName, EventTypeNameResolver.GetName(typeof(EventWithoutAttribute)));
        Assert.Equal("my-stable-alias", EventTypeNameResolver.GetName(typeof(EventWithAlias)));
    }

    [Fact]
    public void Warmup_EmptyList_DoesNotThrow()
    {
        var ex = Record.Exception(() => EventTypeNameResolver.Warmup(Array.Empty<Type>()));
        Assert.Null(ex);
    }
}
