using LawnDart.EventStore;
using LawnDart.Publishing;
using Microsoft.Extensions.DependencyInjection;
using static LawnDart.Tests.Publishing.PublishingFixtures;

namespace LawnDart.Tests.Publishing;

public class PublishEventsRegistrationTests
{
    [Fact]
    public void PublishEvents_registers_the_map_keyed_by_context_and_the_process_catalog()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("membership")
            .WithEventTypes(MembershipTypes)
            .PublishEvents(p => p
                .Publish<OwnerAdded>()
                .Translate<MemberJoined, MemberJoinedNotice>(e =>
                    new MemberJoinedNotice(e.Id, e.Timestamp, e.GroupId, e.UserId, e.Role)));

        using var provider = services.BuildServiceProvider();
        var map = provider.GetRequiredKeyedService<PublishedEventMap>("membership");
        Assert.Equal("membership", map.ContextName);
        Assert.Equal([typeof(OwnerAdded), typeof(MemberJoined)], map.SourceTypes);
        Assert.Null(provider.GetService<PublishedEventMap>());

        var catalog = provider.GetRequiredService<PublishedEventCatalog>();
        Assert.Equivalent(new[] { typeof(OwnerAdded), typeof(MemberJoinedNotice) }, catalog.PublishedTypes);
        Assert.True(catalog.TryResolveType("membership.member-joined", out var type));
        Assert.Equal(typeof(MemberJoinedNotice), type);
        Assert.False(catalog.TryResolveType("membership-member-joined", out _));
        Assert.Null(catalog.UpcastPipeline);
    }

    [Fact]
    public void Default_context_also_registers_the_map_without_a_key()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext(LawnDartNames.DefaultContextName)
            .WithEventTypes(MembershipTypes)
            .PublishEvents(p => p.Publish<OwnerAdded>());

        using var provider = services.BuildServiceProvider();
        Assert.Same(
            provider.GetRequiredKeyedService<PublishedEventMap>(LawnDartNames.DefaultContextName),
            provider.GetRequiredService<PublishedEventMap>());
    }

    [Fact]
    public void Process_catalog_is_the_union_of_every_context()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("membership")
            .WithEventTypes(MembershipTypes)
            .PublishEvents(p => p.Publish<OwnerAdded>());
        services.AddBoundedContext("groups")
            .WithEventTypes(MembershipTypes)
            .PublishEvents(p => p
                .Publish<OwnerAdded>()
                .AddTranslator<MemberRemovedTranslator>());

        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<PublishedEventCatalog>();
        Assert.Equivalent(new[] { typeof(OwnerAdded), typeof(MemberLeftNotice) }, catalog.PublishedTypes);
        Assert.Equal(2, catalog.PublishedTypes.Count);
        Assert.NotSame(
            provider.GetRequiredKeyedService<PublishedEventMap>("membership"),
            provider.GetRequiredKeyedService<PublishedEventMap>("groups"));
    }

    [Fact]
    public void Two_contexts_publishing_one_name_with_different_types_fail_at_startup()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("membership")
            .WithEventTypes(MembershipTypes)
            .PublishEvents(p => p.Translate<MemberJoined, MemberJoinedNotice>(e =>
                new MemberJoinedNotice(e.Id, e.Timestamp, e.GroupId, e.UserId, e.Role)));
        var groups = services.AddBoundedContext("groups").WithEventTypes(MembershipTypes);

        var ex = Assert.Throws<InvalidOperationException>(() => groups.PublishEvents(p => p
            .Translate<MemberJoined, OtherMemberJoinedNotice>(e => new OtherMemberJoinedNotice(e.Id, e.Timestamp))));

        Assert.Contains("'membership.member-joined'", ex.Message);
        Assert.Contains("context 'membership'", ex.Message);
        Assert.Contains("context 'groups'", ex.Message);
    }

    [Fact]
    public void Catalog_create_rejects_name_conflicts_too()
    {
        var a = PublishedEventMap.Create("membership", MembershipCatalog(), p => p
            .Translate<MemberJoined, MemberJoinedNotice>(e => new MemberJoinedNotice(e.Id, e.Timestamp, e.GroupId, e.UserId, e.Role)));
        var b = PublishedEventMap.Create("groups", MembershipCatalog(), p => p
            .Translate<OwnerAdded, OtherMemberJoinedNotice>(e => new OtherMemberJoinedNotice(e.Id, e.Timestamp)));

        var ex = Assert.Throws<InvalidOperationException>(() => PublishedEventCatalog.Create([a, b]));
        Assert.Contains("'membership.member-joined'", ex.Message);
    }

    [Fact]
    public void PublishEvents_requires_WithEventTypes_first()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("membership");

        var ex = Assert.Throws<InvalidOperationException>(() => builder.PublishEvents(p => p.Publish<OwnerAdded>()));
        Assert.Contains("WithEventTypes first", ex.Message);
    }

    [Fact]
    public void PublishEvents_twice_on_one_context_fails()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("membership")
            .WithEventTypes(MembershipTypes)
            .PublishEvents(p => p.Publish<OwnerAdded>());

        var ex = Assert.Throws<InvalidOperationException>(() => builder.PublishEvents(p => p.Publish<RoleChanged>()));
        Assert.Contains("already been called for context 'membership'", ex.Message);
    }

    [Fact]
    public void PublishEvents_that_declares_nothing_lists_the_context_event_types()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("membership").WithEventTypes(MembershipTypes);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.PublishEvents(_ => { }));
        Assert.Contains("'membership' declares no published events", ex.Message);
        Assert.Contains("Event types in this context: membership-member-joined", ex.Message);
        Assert.Contains("membership-role-changed (" + typeof(RoleChanged).FullName + ")", ex.Message);
    }

    [Fact]
    public void Store_startup_message_for_an_outbox_with_nothing_declared_lists_the_event_types()
    {
        var message = BoundedContextExtensions.MissingPublishedEventsMessage("membership", MembershipCatalog());

        Assert.Contains("'membership' declares no published events", message);
        Assert.Contains("PublishEvents(", message);
        Assert.Contains("membership-owner-added (" + typeof(OwnerAdded).FullName + ")", message);
    }

    [Fact]
    public void Store_startup_message_without_a_catalog_still_names_the_context()
    {
        var message = BoundedContextExtensions.MissingPublishedEventsMessage("membership", catalog: null);

        Assert.Contains("'membership'", message);
        Assert.DoesNotContain("Event types in this context", message);
    }

    [Fact]
    public void Failed_PublishEvents_registers_nothing()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("membership").WithEventTypes(MembershipTypes);

        Assert.Throws<InvalidOperationException>(() => builder.PublishEvents(p => p.Publish<InviteSent>()));

        using var provider = services.BuildServiceProvider();
        Assert.Null(provider.GetKeyedService<PublishedEventMap>("membership"));
        Assert.Null(provider.GetService<PublishedEventCatalog>());
    }
}
