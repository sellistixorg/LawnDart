using LawnDart.EventStore;
using Microsoft.Extensions.DependencyInjection;

namespace LawnDart.Tests.EventStore;

public class EventTypeNameResolverTests
{
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

    private record ChildEvent : ParentEvent;

    [EventTypeName("dup-token")]
    private record DupA : LawnDart.IEvent
    {
        public Guid Id { get; init; }
        public DateTime Timestamp { get; init; }
    }

    [EventTypeName("dup-token")]
    private record DupB : LawnDart.IEvent
    {
        public Guid Id { get; init; }
        public DateTime Timestamp { get; init; }
    }

    [Fact]
    public void GetName_WithoutAttribute_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => EventTypeNameResolver.GetName(typeof(EventWithoutAttribute)));
        Assert.Contains("EventTypeName", ex.Message);
        Assert.Contains("FullName is not stored", ex.Message);
    }

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
        Assert.NotEqual(typeof(EventWithAlias).Name, name);
    }

    [Fact]
    public void GetName_ChildOfAttributedParent_Throws_WhenChildHasNoAttribute()
    {
        Assert.Throws<InvalidOperationException>(
            () => EventTypeNameResolver.GetName(typeof(ChildEvent)));
    }

    [Fact]
    public void GetName_CalledMultipleTimes_ReturnsSameStringReference()
    {
        var name1 = EventTypeNameResolver.GetName(typeof(EventWithAlias));
        var name2 = EventTypeNameResolver.GetName(typeof(EventWithAlias));
        Assert.Same(name1, name2);
    }

    [Fact]
    public void TryGetDeclaredName_ReturnsTokenOrNull()
    {
        Assert.Equal("my-stable-alias", EventTypeNameResolver.TryGetDeclaredName(typeof(EventWithAlias)));
        Assert.Null(EventTypeNameResolver.TryGetDeclaredName(typeof(EventWithoutAttribute)));
        Assert.Null(EventTypeNameResolver.TryGetDeclaredName(typeof(ChildEvent)));
    }

    [Fact]
    public void Warmup_ThenGetName_UsesToken_NotFullName()
    {
        EventTypeNameResolver.Warmup([typeof(EventWithAlias)]);
        Assert.Equal("my-stable-alias", EventTypeNameResolver.GetName(typeof(EventWithAlias)));
    }

    [Fact]
    public void Warmup_MissingAttribute_Throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => EventTypeNameResolver.Warmup([typeof(EventWithoutAttribute)]));
    }

    [Fact]
    public void Warmup_NonEvent_Throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => EventTypeNameResolver.Warmup([typeof(string)]));
    }

    [Fact]
    public void Warmup_DuplicateToken_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => EventTypeNameResolver.Warmup([typeof(DupA), typeof(DupB)]));
        Assert.Contains("dup-token", ex.Message);
    }

    [Fact]
    public void Warmup_EmptyList_DoesNotThrow()
    {
        var ex = Record.Exception(() => EventTypeNameResolver.Warmup(Array.Empty<Type>()));
        Assert.Null(ex);
    }

    [Fact]
    public void TryResolveType_AfterWarmup_ResolvesTokenAndFullNameAlias()
    {
        EventTypeNameResolver.Warmup([typeof(EventWithAlias)]);

        Assert.True(EventTypeNameResolver.TryResolveType("my-stable-alias", out var byToken));
        Assert.Equal(typeof(EventWithAlias), byToken);

        Assert.True(EventTypeNameResolver.TryResolveType(typeof(EventWithAlias).FullName!, out var byFullName));
        Assert.Equal(typeof(EventWithAlias), byFullName);

        Assert.True(EventTypeNameResolver.TryResolveType(nameof(EventWithAlias), out var byName));
        Assert.Equal(typeof(EventWithAlias), byName);
    }

    [Fact]
    public void WriteName_IsCatalogToken_FullNameIsReadAliasOnly()
    {
        EventTypeNameResolver.Warmup([typeof(EventWithAlias)]);

        var written = EventTypeNameResolver.GetName(typeof(EventWithAlias));
        Assert.Equal("my-stable-alias", written);
        Assert.NotEqual(typeof(EventWithAlias).FullName, written);

        Assert.True(EventTypeNameResolver.TryResolveType(typeof(EventWithAlias).FullName!, out var aliased));
        Assert.Equal(typeof(EventWithAlias), aliased);
        Assert.Equal(written, EventTypeNameResolver.GetName(aliased));
    }

    [Fact]
    public void WithEventTypes_RejectsNonEvent()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("catalog-test-reject");
        Assert.Throws<InvalidOperationException>(() => builder.WithEventTypes(typeof(string)));
    }

    [Fact]
    public void WithEventTypes_DuplicateToken_Throws()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("catalog-test-dup");
        Assert.Throws<InvalidOperationException>(() => builder.WithEventTypes(typeof(DupA), typeof(DupB)));
    }

    [Fact]
    public void WithEventTypes_RegistersCatalogToken()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("catalog-test-ok").WithEventTypes(typeof(EventWithAlias));
        Assert.True(EventTypeNameResolver.TryResolveType("my-stable-alias", out var type));
        Assert.Equal(typeof(EventWithAlias), type);
    }
}
