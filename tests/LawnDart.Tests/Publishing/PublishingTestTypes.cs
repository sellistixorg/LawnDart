using System.Text.Json;
using System.Text.Json.Serialization;
using LawnDart.EventStore;
using LawnDart.Publishing;
using LawnDart.Serialization;

namespace LawnDart.Tests.Publishing;

// Internal events of a "membership" context. Public so scans can see them.

[EventTypeName("membership-owner-added")]
public sealed record OwnerAdded(Guid Id, DateTime Timestamp, string GroupId, string UserId) : IEvent;

[EventTypeName("membership-member-joined")]
public sealed record MemberJoined(Guid Id, DateTime Timestamp, string GroupId, string UserId, string Role, string InternalNote) : IEvent;

[EventTypeName("membership-member-removed")]
public sealed record MemberRemoved(Guid Id, DateTime Timestamp, string GroupId, string UserId, bool Silent) : IEvent;

[EventTypeName("membership-role-changed")]
public sealed record RoleChanged(Guid Id, DateTime Timestamp, string GroupId, string UserId, string Role) : IEvent;

[EventTypeName("membership-invite-sent", version: 2, current: true)]
public sealed record InviteSent(Guid Id, DateTime Timestamp, string GroupId, string Email) : IEvent;

[EventTypeName("membership-invite-sent", version: 1)]
public sealed record InviteSentV1(Guid Id, DateTime Timestamp, string GroupId) : IEvent;

// Integration events (notices).

[EventTypeName("membership.member-joined")]
public sealed record MemberJoinedNotice(Guid Id, DateTime Timestamp, string GroupId, string UserId, string Role) : IEvent;

[EventTypeName("membership.member-left")]
public sealed record MemberLeftNotice(Guid Id, DateTime Timestamp, string GroupId, string UserId) : IEvent;

[EventTypeName("membership.role-changed", version: 1)]
public sealed record RoleChangedNoticeV1(Guid Id, DateTime Timestamp, string GroupId) : IEvent;

[EventTypeName("membership.role-changed", version: 2, current: true)]
public sealed record RoleChangedNoticeV2(Guid Id, DateTime Timestamp, string GroupId, string Role) : IEvent;

/// <summary>Upcaster for a published event. Found by PublishEvents WithUpcasters.</summary>
public sealed class RoleChangedNoticeV1ToV2 : IEventUpcaster<RoleChangedNoticeV2, RoleChangedNoticeV1>
{
    public RoleChangedNoticeV2 Upcast(RoleChangedNoticeV1 source)
        => new(source.Id, source.Timestamp, source.GroupId, "member");
}

/// <summary>The only exported translator in this test assembly, so AddTranslators scans find exactly it.</summary>
public sealed class MemberRemovedTranslator : IEventTranslator<MemberRemoved, MemberLeftNotice>
{
    public MemberLeftNotice? Translate(MemberRemoved @event, PublishContext context)
        => @event.Silent ? null : new MemberLeftNotice(context.SourceEventId, @event.Timestamp, @event.GroupId, @event.UserId);
}

/// <summary>A notice with no event name. Published types must have one.</summary>
public sealed record UnnamedNotice(Guid Id, DateTime Timestamp) : IEvent;

/// <summary>Another .NET type that claims a notice name already used.</summary>
[EventTypeName("membership.member-joined")]
public sealed record OtherMemberJoinedNotice(Guid Id, DateTime Timestamp) : IEvent;

internal static class PublishingFixtures
{
    public static readonly Type[] MembershipTypes =
        [typeof(OwnerAdded), typeof(MemberJoined), typeof(MemberRemoved), typeof(RoleChanged)];

    public static EventTypeCatalog MembershipCatalog() => EventTypeCatalog.Materialize(MembershipTypes);

    public static PublishContext Context(IEvent source, string sourceType, params string[] tags)
        => new(source.Id, sourceType, tags, "tenant-1", "corr-1", "cause-1", source.Timestamp);

    public static readonly DateTime At = new(2026, 11, 13, 19, 36, 0, DateTimeKind.Utc);
}

internal sealed class StjEventSerializer : IEventSerializer
{
    internal static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public string ContentType => "application/json";

    public ReadOnlyMemory<byte> Serialize(object obj, Type type)
        => JsonSerializer.SerializeToUtf8Bytes(obj, type, Options);

    public object Deserialize(ReadOnlyMemory<byte> data, Type type)
        => JsonSerializer.Deserialize(data.Span, type, Options)
           ?? throw new InvalidOperationException($"Failed to deserialize {type.Name}");
}
