using System.Text;
using System.Text.Json;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Publishing;
using NSubstitute;
using static LawnDart.Tests.Publishing.PublishingFixtures;

namespace LawnDart.Tests.Publishing;

public class OutboundEventTests
{
    private static EventSession Session() => new(new StjEventSerializer(), MembershipCatalog());

    private static PublishedEventMap Map() => PublishedEventMap.Create("membership", MembershipCatalog(), p => p
        .Publish<OwnerAdded>()
        .Translate<MemberJoined, MemberJoinedNotice>((e, ctx) =>
            new MemberJoinedNotice(ctx.SourceEventId, e.Timestamp, e.GroupId, e.UserId, e.Role))
        .AddTranslator<MemberRemovedTranslator>());

    private static EventMetadata FullMetadata() => new()
    {
        UserId = "user-7",
        UserName = "Support Person",
        AccountId = "acct-9",
        TenantId = "tenant-1",
        IpAddress = "203.0.113.5",
        UserAgent = "agent/1.0",
        CorrelationId = "corr-1",
        CausationId = "cause-1",
        AuthorizedBy = "policy-engine",
        AuthorizedAt = At,
        AuthorizationPolicies = ["members.write"],
        TraceId = "trace-1",
        SpanId = "span-1",
        Custom = new Dictionary<string, string> { ["private"] = "value" }
    };

    [Fact]
    public void AppendEvent_has_no_outbound_by_default()
    {
        var envelope = new AppendEvent("membership-owner-added", Encoding.UTF8.GetBytes("{}"));

        Assert.Null(envelope.Outbound);
    }

    [Fact]
    public void AppendEvent_keeps_an_outbound_set_by_a_raw_caller()
    {
        var outbound = new OutboundEvent("membership.member-joined", Encoding.UTF8.GetBytes("{}"), schemaVersion: 0);
        var envelope = new AppendEvent("membership-member-joined", Encoding.UTF8.GetBytes("{}"), outbound: outbound);

        Assert.Same(outbound, envelope.Outbound);
        Assert.Equal(1, outbound.SchemaVersion);
        Assert.Equal(EventCodec.Json, outbound.CodecId);
        Assert.True(outbound.Metadata.IsEmpty);
    }

    [Fact]
    public void OutboundEvent_copies_caller_buffers()
    {
        var payload = Encoding.UTF8.GetBytes("{\"a\":1}");
        var outbound = new OutboundEvent("membership.member-joined", payload);
        payload[0] = (byte)'X';

        Assert.Equal("{\"a\":1}", Encoding.UTF8.GetString(outbound.Payload.Span));
    }

    [Fact]
    public void ToOutboundEvent_carries_only_the_reduced_metadata()
    {
        var source = new MemberJoined(Guid.NewGuid(), At, "g-1", "u-1", "admin", "secret");
        var notice = new MemberJoinedNotice(source.Id, At, "g-1", "u-1", "admin");
        var context = Context(source, "membership-member-joined", "group:g-1");
        var sourceMetadata = FullMetadata();

        var outbound = Session().ToOutboundEvent(notice, context, sourceMetadata);

        Assert.Equal("membership.member-joined", outbound.EventType);
        Assert.Equal(1, outbound.SchemaVersion);
        Assert.Equal(EventCodec.Json, outbound.CodecId);

        using var doc = JsonDocument.Parse(outbound.Metadata);
        var names = doc.RootElement.EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(
            new[] { "CausationId", "CorrelationId", "Custom", "EventId", "SchemaName", "SchemaVersion", "SpanId", "TenantId", "Timestamp", "TraceId" },
            names);

        var metadata = JsonSerializer.Deserialize<EventMetadata>(outbound.Metadata.Span)!;
        Assert.Equal(source.Id.ToString(), metadata.EventId);
        Assert.Equal("tenant-1", metadata.TenantId);
        Assert.Equal("corr-1", metadata.CorrelationId);
        Assert.Equal("cause-1", metadata.CausationId);
        Assert.Equal("trace-1", metadata.TraceId);
        Assert.Equal("span-1", metadata.SpanId);
        Assert.Equal(At, metadata.Timestamp);
        Assert.Equal("membership.member-joined", metadata.SchemaName);
        Assert.Equal(1, metadata.SchemaVersion);
        Assert.Equal(2, metadata.Custom.Count);
        Assert.Equal(source.Id.ToString(), metadata.Custom[EventSession.SourceEventIdKey]);
        Assert.Equal("membership-member-joined", metadata.Custom[EventSession.SourceEventTypeKey]);
        Assert.Equal("lawndart.source-event-id", EventSession.SourceEventIdKey);
        Assert.Equal("lawndart.source-event-type", EventSession.SourceEventTypeKey);

        var text = Encoding.UTF8.GetString(outbound.Metadata.Span);
        Assert.DoesNotContain("user-7", text);
        Assert.DoesNotContain("Support Person", text);
        Assert.DoesNotContain("acct-9", text);
        Assert.DoesNotContain("203.0.113.5", text);
        Assert.DoesNotContain("agent/1.0", text);
        Assert.DoesNotContain("policy-engine", text);
        Assert.DoesNotContain("members.write", text);
        Assert.DoesNotContain("private", text);
    }

    [Fact]
    public void ToOutboundEvent_uses_the_notice_schema_version()
    {
        var notice = new RoleChangedNoticeV2(Guid.NewGuid(), At, "g-1", "admin");
        var context = new PublishContext(notice.Id, "membership-role-changed", [], null, null, null, At);

        var outbound = Session().ToOutboundEvent(notice, context);

        Assert.Equal("membership.role-changed", outbound.EventType);
        Assert.Equal(2, outbound.SchemaVersion);
        var metadata = JsonSerializer.Deserialize<EventMetadata>(outbound.Metadata.Span)!;
        Assert.Equal(2, metadata.SchemaVersion);
        Assert.Null(metadata.TraceId);
    }

    [Fact]
    public void ToOutboundEvent_rejects_a_notice_without_an_event_name()
    {
        var notice = new UnnamedNotice(Guid.NewGuid(), At);
        var context = new PublishContext(notice.Id, "membership-member-joined", [], null, null, null, At);

        Assert.Throws<InvalidOperationException>(() => Session().ToOutboundEvent(notice, context));
    }

    [Fact]
    public async Task Adapter_attaches_outbound_only_to_declared_events()
    {
        var log = CapturingLog(out var captured);
        var adapter = new EventStoreAdapter(log, Session(), publishedEvents: Map());

        var owner = new OwnerAdded(Guid.NewGuid(), At, "g-1", "u-0");
        var joined = new MemberJoined(Guid.NewGuid(), At, "g-1", "u-1", "admin", "only-inside");
        var role = new RoleChanged(Guid.NewGuid(), At, "g-1", "u-1", "owner");
        var silent = new MemberRemoved(Guid.NewGuid(), At, "g-1", "u-2", Silent: true);
        var removed = new MemberRemoved(Guid.NewGuid(), At, "g-1", "u-3", Silent: false);

        await adapter.AppendAsync(
            "group-g-1",
            [
                new TaggedEvent(owner, "group:g-1"),
                new TaggedEvent(joined, "group:g-1", "user:u-1"),
                new TaggedEvent(role, "group:g-1"),
                new TaggedEvent(silent, "group:g-1"),
                new TaggedEvent(removed, "group:g-1")
            ],
            metadata: FullMetadata());

        var envelopes = Assert.Single(captured);
        Assert.Equal(5, envelopes.Count);

        // Pass-through: the row is the internal event's own bytes.
        var ownerOut = Assert.IsType<OutboundEvent>(envelopes[0].Outbound);
        Assert.Equal("membership-owner-added", ownerOut.EventType);
        Assert.Equal(envelopes[0].Payload.ToArray(), ownerOut.Payload.ToArray());

        // Translation: the row is the notice, with no internal-only field.
        var joinedOut = Assert.IsType<OutboundEvent>(envelopes[1].Outbound);
        Assert.Equal("membership.member-joined", joinedOut.EventType);
        var joinedText = Encoding.UTF8.GetString(joinedOut.Payload.Span);
        Assert.DoesNotContain("only-inside", joinedText);
        Assert.DoesNotContain("InternalNote", joinedText);
        var notice = JsonSerializer.Deserialize<MemberJoinedNotice>(joinedOut.Payload.Span, StjEventSerializer.Options)!;
        Assert.Equal(joined.Id, notice.Id);
        var joinedMeta = JsonSerializer.Deserialize<EventMetadata>(joinedOut.Metadata.Span)!;
        Assert.Equal(joined.Id.ToString(), joinedMeta.EventId);
        Assert.Equal("membership-member-joined", joinedMeta.Custom[EventSession.SourceEventTypeKey]);
        Assert.Null(joinedMeta.UserId);

        // Undeclared, and a translation that returned null: no outbound.
        Assert.Null(envelopes[2].Outbound);
        Assert.Null(envelopes[3].Outbound);

        var removedOut = Assert.IsType<OutboundEvent>(envelopes[4].Outbound);
        Assert.Equal("membership.member-left", removedOut.EventType);

        // The stored envelopes themselves are unchanged.
        Assert.Equal(["group:g-1", "user:u-1"], envelopes[1].Tags);
        Assert.Contains("only-inside", Encoding.UTF8.GetString(envelopes[1].Payload.Span));
        var storedMeta = JsonSerializer.Deserialize<EventMetadata>(envelopes[1].Metadata.Span)!;
        Assert.Equal("user-7", storedMeta.UserId);
    }

    [Fact]
    public async Task Adapter_translates_on_the_condition_append_too()
    {
        var log = CapturingLog(out var captured);
        var adapter = new EventStoreAdapter(log, Session(), publishedEvents: Map());
        var joined = new MemberJoined(Guid.NewGuid(), At, "g-1", "u-1", "admin", "x");
        var role = new RoleChanged(Guid.NewGuid(), At, "g-1", "u-1", "owner");

        await adapter.AppendAsync(
            [new TaggedEvent(joined, "group:g-1"), new TaggedEvent(role, "group:g-1")],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType("membership-member-joined"))));

        var envelopes = Assert.Single(captured);
        Assert.Equal("membership.member-joined", envelopes[0].Outbound?.EventType);
        Assert.Null(envelopes[1].Outbound);
        var meta = JsonSerializer.Deserialize<EventMetadata>(envelopes[0].Outbound!.Metadata.Span)!;
        Assert.Null(meta.TenantId);
        Assert.Equal(joined.Id.ToString(), meta.EventId);
    }

    [Fact]
    public async Task Adapter_without_a_map_attaches_nothing()
    {
        var log = CapturingLog(out var captured);
        var adapter = new EventStoreAdapter(log, Session());

        await adapter.AppendAsync(
            "group-g-1",
            [new TaggedEvent(new OwnerAdded(Guid.NewGuid(), At, "g-1", "u-0")), new TaggedEvent(new MemberJoined(Guid.NewGuid(), At, "g-1", "u-1", "admin", "x"))]);

        Assert.All(Assert.Single(captured), e => Assert.Null(e.Outbound));
    }

    private static IEventLog CapturingLog(out List<List<AppendEvent>> captured)
    {
        var calls = new List<List<AppendEvent>>();
        captured = calls;
        var log = Substitute.For<IEventLog>();
        log.AppendAsync(Arg.Any<string>(), Arg.Any<IEnumerable<AppendEvent>>(), Arg.Any<long?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                calls.Add(ci.ArgAt<IEnumerable<AppendEvent>>(1).ToList());
                return Task.FromResult(new AppendResult([1L], null, 1));
            });
        log.AppendAsync(Arg.Any<IEnumerable<AppendEvent>>(), Arg.Any<AppendCondition>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                calls.Add(ci.ArgAt<IEnumerable<AppendEvent>>(0).ToList());
                return Task.FromResult(new AppendResult([1L], null, 1));
            });
        return log;
    }
}
