using LawnDart.EventStore;
using LawnDart.Publishing;
using static LawnDart.Tests.Publishing.PublishingFixtures;

namespace LawnDart.Tests.Publishing;

public class PublishedEventMapTests
{
    [Fact]
    public void Publish_passes_the_same_event_through()
    {
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p.Publish<OwnerAdded>());
        var source = new OwnerAdded(Guid.NewGuid(), At, "g-1", "u-1");

        Assert.True(map.TryTranslate(source, Context(source, "membership-owner-added"), out var notice));
        Assert.Same(source, notice);
        Assert.Equal([typeof(OwnerAdded)], map.SourceTypes);
        Assert.Equal([typeof(OwnerAdded)], map.PublishedTypes);
        Assert.Equal("membership", map.ContextName);
    }

    [Fact]
    public void Translate_without_context_builds_the_notice()
    {
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p
            .Translate<MemberJoined, MemberJoinedNotice>(e =>
                new MemberJoinedNotice(e.Id, e.Timestamp, e.GroupId, e.UserId, e.Role)));
        var source = new MemberJoined(Guid.NewGuid(), At, "g-1", "u-1", "admin", "secret");

        Assert.True(map.TryTranslate(source, Context(source, "membership-member-joined"), out var notice));
        var joined = Assert.IsType<MemberJoinedNotice>(notice);
        Assert.Equal(source.Id, joined.Id);
        Assert.Equal("admin", joined.Role);
        Assert.Equal([typeof(MemberJoinedNotice)], map.PublishedTypes);
    }

    [Fact]
    public void Translate_with_context_reads_the_publish_context()
    {
        PublishContext seen = default;
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p
            .Translate<MemberJoined, MemberJoinedNotice>((e, ctx) =>
            {
                seen = ctx;
                return new MemberJoinedNotice(ctx.SourceEventId, ctx.Timestamp, e.GroupId, e.UserId, e.Role);
            }));
        var source = new MemberJoined(Guid.NewGuid(), At, "g-1", "u-1", "admin", "secret");

        Assert.True(map.TryTranslate(source, Context(source, "membership-member-joined", "group:g-1"), out var notice));
        Assert.Equal(source.Id, notice.Id);
        Assert.Equal(source.Id, seen.SourceEventId);
        Assert.Equal("membership-member-joined", seen.SourceEventType);
        Assert.Equal(["group:g-1"], seen.Tags);
        Assert.Equal("tenant-1", seen.TenantId);
        Assert.Equal("corr-1", seen.CorrelationId);
        Assert.Equal("cause-1", seen.CausationId);
        Assert.Equal(At, seen.Timestamp);
    }

    [Fact]
    public void Translate_returning_null_publishes_nothing()
    {
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p
            .Translate<MemberJoined, MemberJoinedNotice>(_ => null));
        var source = new MemberJoined(Guid.NewGuid(), At, "g-1", "u-1", "admin", "secret");

        Assert.False(map.TryTranslate(source, Context(source, "membership-member-joined"), out var notice));
        Assert.Null(notice);
    }

    [Fact]
    public void Undeclared_event_is_not_published()
    {
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p.Publish<OwnerAdded>());
        var source = new RoleChanged(Guid.NewGuid(), At, "g-1", "u-1", "admin");

        Assert.False(map.TryTranslate(source, Context(source, "membership-role-changed"), out var notice));
        Assert.Null(notice);
    }

    [Fact]
    public void AddTranslator_uses_the_translator_and_its_null_means_skip()
    {
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p
            .AddTranslator<MemberRemovedTranslator>());
        var loud = new MemberRemoved(Guid.NewGuid(), At, "g-1", "u-1", Silent: false);
        var silent = new MemberRemoved(Guid.NewGuid(), At, "g-1", "u-2", Silent: true);

        Assert.True(map.TryTranslate(loud, Context(loud, "membership-member-removed"), out var notice));
        Assert.Equal(new MemberLeftNotice(loud.Id, At, "g-1", "u-1"), notice);
        Assert.False(map.TryTranslate(silent, Context(silent, "membership-member-removed"), out _));
    }

    [Fact]
    public void AddTranslators_scan_finds_exported_translators()
    {
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p
            .AddTranslators<PublishedEventMapTests>());

        Assert.Equal([typeof(MemberRemoved)], map.SourceTypes);
        Assert.Equal([typeof(MemberLeftNotice)], map.PublishedTypes);
    }

    [Fact]
    public void AddTranslators_scan_that_finds_nothing_throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", MembershipCatalog(), p => p
                .AddTranslators(typeof(IEvent).Assembly)));

        Assert.Contains("AddTranslators scanned assembly 'LawnDart'", ex.Message);
        Assert.Contains("IEventTranslator<,>", ex.Message);
    }

    [Fact]
    public void AddTranslator_without_parameterless_constructor_throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", MembershipCatalog(), p => p
                .AddTranslator<TranslatorWithService>()));

        Assert.Contains("public parameterless constructor", ex.Message);
    }

    [Fact]
    public void Translation_that_returns_another_type_throws()
    {
        var map = PublishedEventMap.Create("membership", MembershipCatalog(), p => p
            .Translate<MemberJoined, ShimNotice>(e => new OtherShim(e.Id, e.Timestamp)));
        var source = new MemberJoined(Guid.NewGuid(), At, "g-1", "u-1", "admin", "secret");

        var ex = Assert.Throws<InvalidOperationException>(() =>
            map.TryTranslate(source, Context(source, "membership-member-joined"), out _));
        Assert.Contains(typeof(OtherShim).FullName!, ex.Message);
    }

    [Fact]
    public void Published_type_without_event_name_fails()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", MembershipCatalog(), p => p
                .Translate<MemberJoined, UnnamedNotice>(e => new UnnamedNotice(e.Id, e.Timestamp))));

        Assert.Contains(typeof(UnnamedNotice).FullName!, ex.Message);
        Assert.Contains("[EventTypeName(", ex.Message);
    }

    [Fact]
    public void Source_type_not_in_the_context_catalog_fails()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", MembershipCatalog(), p => p.Publish<InviteSent>()));

        Assert.Contains("'membership'", ex.Message);
        Assert.Contains(typeof(InviteSent).FullName!, ex.Message);
        Assert.Contains("WithEventTypes catalog", ex.Message);
    }

    [Fact]
    public void Older_schema_version_as_source_fails()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(InviteSent), typeof(InviteSentV1)]);
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", catalog, p => p.Publish<InviteSentV1>()));

        Assert.Contains("older schema version", ex.Message);
        Assert.Contains(typeof(InviteSent).FullName!, ex.Message);
    }

    [Fact]
    public void Same_source_declared_twice_fails()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", MembershipCatalog(), p => p
                .Publish<MemberRemoved>()
                .AddTranslator<MemberRemovedTranslator>()));

        Assert.Contains("twice", ex.Message);
        Assert.Contains("at most one published event", ex.Message);
    }

    [Fact]
    public void Two_notice_types_with_one_name_in_one_context_fail()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", MembershipCatalog(), p => p
                .Translate<MemberJoined, MemberJoinedNotice>(e => new MemberJoinedNotice(e.Id, e.Timestamp, e.GroupId, e.UserId, e.Role))
                .Translate<OwnerAdded, OtherMemberJoinedNotice>(e => new OtherMemberJoinedNotice(e.Id, e.Timestamp))));

        Assert.Contains("'membership.member-joined'", ex.Message);
    }

    [Fact]
    public void Nothing_declared_fails_and_lists_the_context_event_types()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            PublishedEventMap.Create("membership", MembershipCatalog(), _ => { }));

        Assert.Contains("'membership' declares no published events", ex.Message);
        Assert.Contains("membership-member-joined (" + typeof(MemberJoined).FullName + ")", ex.Message);
        Assert.Contains("membership-owner-added", ex.Message);
        Assert.Contains("membership-member-removed", ex.Message);
        Assert.Contains("membership-role-changed", ex.Message);
    }

    /// <summary>A declared target whose subtype is returned at runtime.</summary>
    [EventTypeName("membership.shim")]
    public record ShimNotice(Guid Id, DateTime Timestamp) : IEvent;

    public sealed record OtherShim(Guid Id, DateTime Timestamp) : ShimNotice(Id, Timestamp);

    private sealed class TranslatorWithService : IEventTranslator<MemberJoined, MemberJoinedNotice>
    {
        private readonly string _prefix;

        public TranslatorWithService(string prefix) => _prefix = prefix;

        public MemberJoinedNotice? Translate(MemberJoined @event, PublishContext context)
            => new(@event.Id, @event.Timestamp, _prefix + @event.GroupId, @event.UserId, @event.Role);
    }
}
