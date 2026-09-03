# Tagging

Tags are string keys stored with events so DCB (and some queries) can load a
cross-stream slice.

## Conventions

| Kind | Example |
|---|---|
| Identity | `student:0ad2f06e-fc65-4394-bfa1-f3116bf68808` |
| Type | `section:f81ccc2f-61e6-46da-bcb4-d8eec7f0220e` |
| Tenant (optional) | `tenant:academy-tenant` |

Keep tags stable. Do not put free-text names in tags.

## Providing tags

Register `ITagProvider` globally (`AddTagProvider<T>`) or per context
(`WithTagProvider`). The provider inspects the event (and optional command) and
returns tag strings.

InMemory and SQL Server both honor tags on append. DCB `AppendCondition` uses
the same tag set the entity loaded.

## What tags are not

Tags are not stream IDs. A traditional aggregate still writes one stream
(`tenant:Student:{id}`). DCB may write events that carry multiple tags and
share a DCB stream identity — see [STREAM_IDS.md](STREAM_IDS.md).
