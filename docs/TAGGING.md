# Tagging

A tag is an exact string stored on an event. The context's `ITagProvider`
is the source of that context's tags. Decision code and query code call
the same format the provider writes.

Tags choose the events a Dynamic Consistency Boundary (DCB) decision
loads, and they are the fence on `AppendCondition`. They are also the
key a later search or index uses. The string is data.

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

Interpolating a `Guid` uses format `D` (hyphens). Use that form.
Format `N` is a different string.

Stream IDs stay `{tenant}:{Type}:{id}`. The type segment there is the
aggregate type name. A tag uses the lowercase type above.
See [STREAM_IDS.md](STREAM_IDS.md).

## One format

`ITagProvider` has one method, `GetTags(IEvent, object?)`. It runs when
the repository appends. It needs an event, so it cannot build the load
query from an id. `GetOrCreateEntityAsync`, `QueryItem.ByTags`, `Emit`,
`GetTagsForProjection`, and `GetTagsForReaction` take the strings you
supply.

Put the format methods on the provider type. `GetTags` returns them.
The decision calls them too.

```csharp
public sealed class AcademyTagProvider : ITagProvider
{
    public static string Student(Guid studentId) => $"student:{studentId}";

    public static string Section(Guid sectionId) => $"section:{sectionId}";

    public static string Course(Guid courseId) => $"course:{courseId}";

    public static string[] ForEnrollment(Guid studentId, Guid sectionId)
        => [Student(studentId), Section(sectionId)];

    public IEnumerable<string> GetTags(IEvent @event, object? context = null) => @event switch
    {
        StudentRegistered e => [Student(e.StudentId)],
        SectionCreated e => [Section(e.SectionId), Course(e.CourseId)],
        SeatReserved e => [Student(e.StudentId), Section(e.SectionId)],
        StudentEnrolled e => [Student(e.StudentId), Course(e.CourseId)],
        _ => []
    };
}
```

Register that provider for one context:

```csharp
services.AddBoundedContext("academy")
    .UseInMemory()
    .WithTagProvider<AcademyTagProvider>();
```

`AddTagProvider<AcademyTagProvider>()` registers a provider for every
context that has no `WithTagProvider`. `WithTagProvider` also accepts
an instance or a factory. The repository uses the keyed provider when
one is registered, and otherwise the global provider.

Static methods need no further registration. When a format method needs
a service, register the concrete type and pass that instance to the
`WithTagProvider` factory. Query code then injects the same object.
Resolving `ITagProvider` only gives you `GetTags`.

Load with the provider's strings:

```csharp
var tags = AcademyTagProvider.ForEnrollment(studentId, sectionId);
var entity = await dcb.GetOrCreateEntityAsync<EnrollmentEntity>(tags);
```

`Emit` takes the same methods. `SeatReserved` has no course id, and the
DCB repository calls `GetTags` with a null context, so the course tag
has to be passed here:

```csharp
Emit(seatReserved,
    AcademyTagProvider.Student(studentId),
    AcademyTagProvider.Section(sectionId),
    AcademyTagProvider.Course(courseId));
```

A query uses the same method:

```csharp
var query = Query.FromItems(
    QueryItem.ByTags(AcademyTagProvider.Student(studentId)));
```

One `QueryItem` matches an event that has every listed tag. Several
items in one `Query` match an event that fits any item.
`DcbRepository` loads with one item, so the load requires every tag.
`DcbProjector.InitializeFromHistoryAsync` makes one item per tag, so
that replay matches any of them. `GetTagsForProjection` and
`GetTagsForReaction` return the provider's strings.

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
cannot format an id, so identity tags stay on the provider.

When `EnforceDcbTenantIsolation` is on, the DCB repository adds
`tenant:{tenantId}` when no tag starts with `tenant:`. The prefix
check ignores case. Write `tenant:` in lowercase from the provider so
the stored tag and the query are the same string.

Academy `EnrollmentEntity.GetTags` and its `Emit` calls already use
these lowercase prefixes. Call the provider methods from both sites.
See [DCB patterns](DCB_PATTERNS.md).

## Common mistakes

A second literal. The provider writes `student:{id}` and a handler
queries `Student:{id}`. The decision loads an empty entity. The fence
does not see the other writer's events.

A different `Guid` format. `student:{id:N}` and `student:{id}` are
different tags.

A load of `student:{id}` and `section:{id}` while the bootstrap event
was stored with only `student:{id}`. One query item requires every tag.
The event is in the log and absent from the decision.

A query built by calling `GetTags`. `GetTags` needs an event and runs
at append. The load and the fence use the array passed to
`GetOrCreateEntityAsync`. Build that array with the provider's format
methods.

A `Tenant:` prefix. Isolation sees a tenant prefix and skips
`tenant:{id}`. A query for `tenant:{id}` does not match `Tenant:{id}`.

The stream-id type copied into the tag (`Student:` taken from
`academy-tenant:Student:{id}`).

A display name, a space, or other free text in the tag. Renaming the
person leaves the old key in the log.
