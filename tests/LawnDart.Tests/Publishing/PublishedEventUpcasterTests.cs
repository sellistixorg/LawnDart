using LawnDart.EventStore;
using LawnDart.Publishing;
using Microsoft.Extensions.DependencyInjection;
using static LawnDart.Tests.Publishing.PublishingFixtures;

namespace LawnDart.Tests.Publishing;

public class PublishedEventUpcasterTests
{
    [Fact]
    public void WithUpcasters_adds_older_notice_versions_and_their_chain()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("membership")
            .WithEventTypes(MembershipTypes)
            .PublishEvents(p => p
                .Translate<RoleChanged, RoleChangedNoticeV2>(e => new RoleChangedNoticeV2(e.Id, e.Timestamp, e.GroupId, e.Role))
                .WithUpcasters<PublishedEventUpcasterTests>());

        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<PublishedEventCatalog>();

        Assert.Equal([typeof(RoleChangedNoticeV2)], catalog.PublishedTypes);
        Assert.True(catalog.TryResolveType("membership.role-changed", 1, out var v1));
        Assert.Equal(typeof(RoleChangedNoticeV1), v1);
        Assert.True(catalog.TryResolveType("membership.role-changed", out var current));
        Assert.Equal(typeof(RoleChangedNoticeV2), current);

        var pipeline = Assert.IsType<EventUpcastPipeline>(catalog.UpcastPipeline);
        var upcast = pipeline.UpcastToCurrent(new RoleChangedNoticeV1(Guid.NewGuid(), At, "g-1"));
        Assert.Equal("member", Assert.IsType<RoleChangedNoticeV2>(upcast).Role);
    }

    [Fact]
    public void WithUpcasters_ignores_upcasters_for_events_that_are_not_published()
    {
        // The test assembly also holds upcasters for internal "author" events; none are kept.
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p
            .Publish<OwnerAdded>()
            .WithUpcasters<PublishedEventUpcasterTests>());
        var catalog = PublishedEventCatalog.Create([map]);

        Assert.Null(catalog.UpcastPipeline);
        Assert.False(catalog.TryResolveType("membership.role-changed", out _));
    }

    [Fact]
    public void Without_upcasters_an_older_notice_version_is_not_readable()
    {
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p
            .Translate<RoleChanged, RoleChangedNoticeV2>(e => new RoleChangedNoticeV2(e.Id, e.Timestamp, e.GroupId, e.Role)));
        var catalog = PublishedEventCatalog.Create([map]);

        Assert.False(catalog.TryResolveType("membership.role-changed", 1, out _));
        Assert.Null(catalog.UpcastPipeline);
    }

    [Fact]
    public void Publishing_an_older_notice_version_fails()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", MembershipCatalog(), p => p
                .Translate<RoleChanged, RoleChangedNoticeV1>(e => new RoleChangedNoticeV1(e.Id, e.Timestamp, e.GroupId))
                .WithUpcasters<PublishedEventUpcasterTests>()));

        Assert.Contains("older", ex.Message);
        Assert.Contains(typeof(RoleChangedNoticeV2).FullName!, ex.Message);
    }

    [Fact]
    public void WithUpcasters_twice_fails()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", MembershipCatalog(), p => p
                .Publish<OwnerAdded>()
                .WithUpcasters<PublishedEventUpcasterTests>()
                .WithUpcasters<PublishedEventUpcasterTests>()));

        Assert.Contains("WithUpcasters has already been called for context 'membership'", ex.Message);
    }
}
