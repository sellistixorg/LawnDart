# Agent instructions

## Commits

The git author of a cloud agent commit is `Cursor Agent <cursoragent@cursor.com>`.
Every commit message must include a trailer that matches that author:

Signed-off-by: Cursor Agent <cursoragent@cursor.com>

`git commit -s` signs off as the configured user, which is not that author.
Add the author line explicitly. An extra sign-off for the connected GitHub user can stay in the message.

Subject: imperative mood, 72 characters or less, no trailing period.
The body says why. Leave a blank line before `Signed-off-by`.

## Scope

InMemory and SQL Server are the stores. PostgreSQL support is in progress for the 0.7 line. Transports are `LawnDart.Messaging.InMemory`, `LawnDart.Messaging.RabbitMQ`, `LawnDart.Messaging.AzureServiceBus`, and `LawnDart.Messaging.AwsSqs`.
Reshaping public API needs an issue first.
Bug fixes, tests, docs, and work behind existing abstractions do not.

## Verify before you finish

Default check is unit tests, with no Docker:

```bash
dotnet test --filter "Category!=Integration"
```

SQL tests are `Category=Integration` and need Docker.
The InMemory-to-SQL swap and the event-log contract are `Category=Contract`.
Public and protected members need a `<summary>`.
A user-facing change gets a bullet under `## [Unreleased]` in `CHANGELOG.md`.
Do not add a version heading.

## Dependencies and versions

Runtime dependencies stay limited to `Microsoft.Extensions.*`, `Microsoft.AspNetCore.*`, `Microsoft.Data.SqlClient`, `Npgsql` (the .NET driver for PostgreSQL, for the Postgres packages), `RabbitMQ.Client`, `Azure.Messaging.ServiceBus`, and `OpenTelemetry`.
A new one needs an issue, and its version is pinned in `Directory.Packages.props`.
Package versions come from MinVer tags. Do not set `<Version>` in a project file.
Do not commit `bin/` or `obj/`.

## Where to read more

`CONTRIBUTING.md` is the definition of done.
`.cursor/rules/docs-voice.mdc` applies when editing `docs/`, `README.md`, or `index.md`.
`skills/BUILD_KIT.md` applies only when the task is a consumer app.
Code under `src/` is the authority for types and behavior.
