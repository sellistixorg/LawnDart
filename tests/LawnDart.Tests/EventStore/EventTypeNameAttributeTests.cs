using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public class EventTypeNameAttributeTests
{
    [Fact]
    public void OneArg_IsVersion1AndNotExplicitCurrent()
    {
        var attr = new EventTypeNameAttribute("author-registered");

        Assert.Equal("author-registered", attr.Name);
        Assert.Equal(1, attr.Version);
        Assert.False(attr.Current);
    }

    [Fact]
    public void Versioned_DefaultsCurrentToFalse()
    {
        var attr = new EventTypeNameAttribute("author-registered", version: 2);

        Assert.Equal("author-registered", attr.Name);
        Assert.Equal(2, attr.Version);
        Assert.False(attr.Current);
    }

    [Fact]
    public void Versioned_CanMarkCurrent()
    {
        var attr = new EventTypeNameAttribute("author-registered", version: 2, current: true);

        Assert.Equal(2, attr.Version);
        Assert.True(attr.Current);
    }

    [Fact]
    public void VersionLessThanOne_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new EventTypeNameAttribute("author-registered", version: 0));
    }

    [Fact]
    public void NullName_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new EventTypeNameAttribute(null!));
    }

    [Fact]
    public void DeclaredOnType_OneArg_ReadsVersion1()
    {
        var attr = (EventTypeNameAttribute)Attribute.GetCustomAttribute(
            typeof(LegacyCurrent), typeof(EventTypeNameAttribute))!;

        Assert.Equal("author-registered", attr.Name);
        Assert.Equal(1, attr.Version);
        Assert.False(attr.Current);
    }

    [Fact]
    public void Warmup_OneArgType_StillSucceeds()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(LegacyCurrent)]);
        Assert.Equal("author-registered", catalog.GetName(typeof(LegacyCurrent)));
    }

    [EventTypeName("author-registered")]
    private sealed record LegacyCurrent(Guid Id, DateTime Timestamp) : LawnDart.IEvent;
}
