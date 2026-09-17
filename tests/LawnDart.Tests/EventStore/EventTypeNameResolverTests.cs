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
        var catalog = EventTypeCatalog.Materialize([]);
        var ex = Assert.Throws<InvalidOperationException>(
            () => catalog.GetName(typeof(EventWithoutAttribute)));
        Assert.Contains("EventTypeName", ex.Message);
        Assert.Contains("FullName is not stored", ex.Message);
    }

    [Fact]
    public void GetName_WithAttribute_ReturnsAttributeValue()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(EventWithAlias)]);
        Assert.Equal("my-stable-alias", catalog.GetName(typeof(EventWithAlias)));
    }

    [Fact]
    public void GetName_WithAttribute_DoesNotReturnFullName()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(EventWithAlias)]);
        var name = catalog.GetName(typeof(EventWithAlias));
        Assert.NotEqual(typeof(EventWithAlias).FullName, name);
        Assert.NotEqual(typeof(EventWithAlias).Name, name);
    }

    [Fact]
    public void GetName_ChildOfAttributedParent_Throws_WhenChildHasNoAttribute()
    {
        var catalog = EventTypeCatalog.Materialize([]);
        Assert.Throws<InvalidOperationException>(
            () => catalog.GetName(typeof(ChildEvent)));
    }

    [Fact]
    public void TryGetDeclaredName_ReturnsTokenOrNull()
    {
        Assert.Equal("my-stable-alias", EventTypeCatalog.TryGetDeclaredName(typeof(EventWithAlias)));
        Assert.Null(EventTypeCatalog.TryGetDeclaredName(typeof(EventWithoutAttribute)));
        Assert.Null(EventTypeCatalog.TryGetDeclaredName(typeof(ChildEvent)));
    }

    [Fact]
    public void Materialize_MissingAttribute_Throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => EventTypeCatalog.Materialize([typeof(EventWithoutAttribute)]));
    }

    [Fact]
    public void Materialize_NonEvent_Throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => EventTypeCatalog.Materialize([typeof(string)]));
    }

    [Fact]
    public void Materialize_RawRecordedEvent_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => EventTypeCatalog.Materialize([typeof(RawRecordedEvent)]));
        Assert.Contains(nameof(IRawEvent), ex.Message);
    }

    [Fact]
    public void Materialize_DuplicateToken_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => EventTypeCatalog.Materialize([typeof(DupA), typeof(DupB)]));
        Assert.Contains("dup-token", ex.Message);
    }

    [Fact]
    public void Materialize_EmptyList_DoesNotThrow()
    {
        var ex = Record.Exception(() => EventTypeCatalog.Materialize([]));
        Assert.Null(ex);
    }

    [Fact]
    public void TryResolveType_ResolvesToken_NotFullNameOrSimpleName()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(EventWithAlias)]);

        Assert.True(catalog.TryResolveType("my-stable-alias", out var byToken));
        Assert.Equal(typeof(EventWithAlias), byToken);

        Assert.False(catalog.TryResolveType(typeof(EventWithAlias).FullName!, out _));
        Assert.False(catalog.TryResolveType(nameof(EventWithAlias), out _));
    }

    [Fact]
    public void TryResolveType_UsesSchemaVersion()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(EventWithAlias)]);

        Assert.True(catalog.TryResolveType("my-stable-alias", 1, out var v1));
        Assert.Equal(typeof(EventWithAlias), v1);
        Assert.False(catalog.TryResolveType("my-stable-alias", 2, out _));
    }

    [Fact]
    public void FullNameToken_FailsClosed()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(EventWithAlias)]);
        Assert.False(catalog.TryResolveType(typeof(EventWithAlias).FullName!, 1, out _));
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
        using var sp = services.BuildServiceProvider();
        var catalog = sp.GetRequiredKeyedService<IEventTypeCatalog>("catalog-test-ok");
        Assert.True(catalog.TryResolveType("my-stable-alias", out var type));
        Assert.Equal(typeof(EventWithAlias), type);
    }
}
