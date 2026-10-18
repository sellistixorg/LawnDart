# Tagging

A tag is an exact string stored on an event. Build every tag through the
context's `ITagFormat`, in one place. A provider and the decision code
call that same format.

Tags choose the events a Dynamic Consistency Boundary (DCB) decision
loads, and they are the fence on `AppendCondition`. They are also the
key a later search or index uses. The string is data. A second spelling
misses the event and misses the fence.

## Exact match

A load, a fence, and an index compare the whole string.
`student:0ad2f06e-fc65-4394-bfa1-f3116bf68808` matches only that spelling.
`Student:0ad2f06e-fc65-4394-bfa1-f3116bf68808` is a different tag.

SQL Server stores `EventTags.Tag` with the `Latin1_General_100_BIN2`
collation. In-memory reads and subscription filters use ordinal
comparison (`EventQueryMatcher`). `GetStreamsByTagAsync` uses the same
key.

`GetOrCreateEntityAsync` loads with one `QueryItem.ByTags`. The event
is in the decision only when it carries every tag in that item.
`HandleCommandAsync` builds `AppendCondition.FailIfMatches` from those
load tags (`ConsistencyTags`). An event written with a different
spelling is absent from the decision, and it does not trip the fence.
Two decisions can both commit.

## Casing

Write a lowercase type, a colon, and the id.

| Kind | Tag |
|---|---|
| Identity | `student:0ad2f06e-fc65-4394-bfa1-f3116bf68808` |
| Related identity | `section:f81ccc2f-61e6-46da-bcb4-d8eec7f0220e` |
| Tenant | `tenant:academy-tenant` |

`LowercaseTagFormat` writes that shape. A `Guid` uses format `D`
(hyphens). A string id is kept as given, so a tenant code is not
rewritten. Format `N` is a different string.

Stream IDs stay `{tenant}:{Type}:{id}`. The type segment there is the
aggregate type name. A tag uses the lowercase type above.
See [STREAM_IDS.md](STREAM_IDS.md).

## One format

`ITagFormat` has `Tag(type, Guid)` and `Tag(type, string)`.
`AddLawnDart` registers `LowercaseTagFormat` as the unkeyed format
unless you already registered one. `AddTagFormat<T>()` replaces that
global format. `AddTagFormat<T>(contextName)` and `WithTagFormat<T>()`
register a format for one context. The repository uses the keyed format
when one is registered, and otherwise the global format. `WithTagFormat`
also accepts an instance or a factory.

`ITagProvider.GetTags(IEvent, object?)` still runs only on append. It
needs an event, so it cannot build the load from an id. Put the domain
helpers on the concrete provider. `GetTags` returns the same strings
the decision passes to the load and to `Emit`.

```csharp
public sealed class AcademyTagProvider : TagProvider
{
    public AcademyTagProvider(ITagFormat format) : base(format) { }

    public string[] ForEnrollment(Guid studentId, Guid sectionId)
        => [Tag("student", studentId), Tag("section", sectionId)];

    public string Course(Guid courseId) => Tag("course", courseId);

    public override IEnumerable<string> GetTags(IEvent @event, object? context = null) => @event switch
    {
        StudentRegistered e => [Tag("student", e.StudentId)],
        SectionCreated e => [Tag("section", e.SectionId), Tag("course", e.CourseId)],
        SeatReserved e => [Tag("student", e.StudentId), Tag("section", e.SectionId)],
        StudentEnrolled e => [Tag("student", e.StudentId), Tag("course", e.CourseId)],
        _ => []
    };
}
```

Register the format and the provider for one context:

```csharp
services.AddBoundedContext("academy")
    .UseInMemory()
    .WithTagFormat<LowercaseTagFormat>()
    .WithTagProvider<AcademyTagProvider>();
```

`WithTagProvider<AcademyTagProvider>()` injects the unkeyed `ITagFormat`.
`AddLawnDart` registered that one. A context with its own format passes
the keyed instance into the provider:

```csharp
services.AddBoundedContext("academy")
    .UseInMemory()
    .WithTagFormat<AcademyTagFormat>()
    .WithTagProvider(sp => new AcademyTagProvider(
        sp.GetRequiredKeyedService<ITagFormat>("academy")));
```

Load with the provider's strings:

```csharp
var tags = provider.ForEnrollment(studentId, sectionId);
var entity = await dcb.GetOrCreateEntityAsync<EnrollmentEntity>(tags);
```

`Emit` takes the same format. `SeatReserved` has no course id, and the
DCB repository calls `GetTags` with a null context, so the course tag
has to be passed here:

```csharp
Emit(seatReserved,
    provider.Tag("student", studentId),
    provider.Tag("section", sectionId),
    provider.Tag("course", courseId));
```

A query uses the same method:

```csharp
var query = Query.FromItems(
    QueryItem.ByTags(provider.Tag("student", studentId)));
```

One `QueryItem` matches an event that has every listed tag. Several
items in one `Query` match an event that fits any item.
`DcbRepository` loads with one item, so the load requires every tag.
`DcbProjector.InitializeFromHistoryAsync` makes one item per tag, so
that replay matches any of them. `GetTagsForProjection` and
`GetTagsForReaction` return the provider's strings. Call
`UseTagFormat` on the projector or reactor with the same format.

Pass `ForEnrollment` on the bootstrap append as well as on the load.
`GetTags` for `StudentRegistered` returns `student:{id}` only. A load
of `student:{id}` and `section:{id}` still requires both on the stored
event. `AppendWithContextAsync` unions `GetTags` onto the tags you
passed, then the store writes that one list on every event in the call.
On the DCB path the provider is called with a null context. On the
aggregate path the context is the aggregate. The fence stays the tags
you passed to `GetOrCreateEntityAsync`. Tags the provider adds do not
change the load and do not change the fence.

`AggregateRepository` also copies `[Tag]` values. When no tag starts
with `{AggregateType}:`, it adds `{AggregateType}:{id}` from the CLR
type name (`Student:{id}`). That prefix check is ordinal. A provider
tag `student:{id}` does not suppress `Student:{id}`, so the event can
carry both. Query the provider's string. `[Tag]` is a fixed string and
cannot format an id, so identity tags stay on the format.

When `EnforceDcbTenantIsolation` is on, the DCB repository adds
`tenant:{tenantId}` when no tag starts with `tenant:`. The prefix
check ignores case. Write `tenant:` in lowercase from the format so
the stored tag and the query are the same string.

Academy `EnrollmentEntity.GetTags` and its `Emit` calls already use
these lowercase prefixes. Call the provider methods from both sites.
See [DCB patterns](DCB_PATTERNS.md).

## Unknown tags

`WarnOnUnknownTag` defaults to true. Set it on `EventSourcingOptions`:

```csharp
services.Configure<EventSourcingOptions>(o => o.WarnOnUnknownTag = false);
```

`UseInMemory(o => ...)` applies that callback to the same options.
`UseSqlServer` copies `WarnOnUnknownTag = false` and
`FormatAggregateFallbackTag = true` onto those options.

The repository logs a warning when a load tag or a fence tag does not
round-trip through the context format. Loads are `GetStateAsync`,
`GetOrCreateEntityAsync`, and `GetLastSequencePositionAsync`
(`QueryItem.ByTags`). The fence is `AppendCondition.FailIfMatches`,
including the one `HandleCommandAsync` builds and any condition passed
to `AppendWithContextAsync`. `DcbProjector.InitializeFromHistoryAsync`
and `DcbReactor.ReactAsync` check their tags after `UseTagFormat`.

The log is once per distinct tag for the process. It goes to
`ILogger<DcbRepository>`, or to the logger you passed to `UseTagFormat`.
With no logger, or with no `ITagFormat`, the check is skipped. The
check does not throw. It does not change which events match. Set
`WarnOnUnknownTag` to false to skip it.

A tag round-trips when it equals `ITagFormat.Tag` for its type and id.
The type is the text before the first colon. The id is the rest. An id
that parses as a `Guid` uses the `Guid` overload. `student:{guid}`
passes the default format. `Student:{guid}` does not. `student:{guid:N}`
does not. `tenant:academy-tenant` passes when the type is lowercase.
The id is kept as given. A 32-hex-digit id is treated as a `Guid`.

## Aggregate fallback

`FormatAggregateFallbackTag` defaults to false. The repository adds
`{ClrTypeName}:{id}` (`Student:{id}`) when no tag starts with that
prefix.

Leave the flag false on a store that already has events. The old tag
stays on those events. A query for the formatted tag does not see them.
Set the flag only on a new store, where every fallback tag will use the
format.

When the flag is true, that fallback is `ITagFormat.Tag(clrTypeName, id)`.
An id that parses as a `Guid` uses the `Guid` overload. Any other id
uses the string overload. With no format registered, the fallback uses
`LowercaseTagFormat`.

```csharp
services.Configure<EventSourcingOptions>(o => o.FormatAggregateFallbackTag = true);
```

## Common mistakes

A second literal. The provider writes `student:{id}` and a handler
queries `Student:{id}`. The decision loads an empty entity. The fence
does not see the other writer's events. With `WarnOnUnknownTag` left
on, that query logs once.

A different `Guid` format. `student:{id:N}` and `student:{id}` are
different tags.

A load of `student:{id}` and `section:{id}` while the bootstrap event
was stored with only `student:{id}`. One query item requires every tag.
The event is in the log and absent from the decision.

A query built by calling `GetTags`. `GetTags` needs an event and runs
at append. The load and the fence use the array passed to
`GetOrCreateEntityAsync`. Build that array with the provider.

A `Tenant:` prefix. Isolation sees a tenant prefix and skips
`tenant:{id}`. A query for `tenant:{id}` does not match `Tenant:{id}`.

The stream-id type copied into the tag (`Student:` taken from
`academy-tenant:Student:{id}`).

Turning `FormatAggregateFallbackTag` on for a store that already has
`Student:{id}` events. New events land under `student:{id}`. The old
events stay where they were.

A display name, a space, or other free text in the tag. Renaming the
person leaves the old key in the log.
