# Changelog

All notable changes to LawnDart are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is below `1.0.0`, a **minor** bump may contain breaking
changes to the public API.

## [Unreleased]

### Breaking

- An in-memory append checks the tenant id the same way a SQL Server append does. `InMemoryEventStoreOptions.RequireTenantId` defaults to true. `UseInMemory` copies `LawnDartOptions.RequireTenantId` and `ValidateNames` onto the store. A hosted in-memory app, and a store constructed with `new InMemoryEventStore()`, rejects a stream id with no tenant prefix and a dynamic consistency boundary append with no `TenantId`. An append whose stream id and event metadata name two different tenant ids throws. That check also runs when `RequireTenantId` is false. To upgrade, add a tenant prefix and `TenantId`, or set `RequireTenantId` to false. Set `ValidateNames` to false to keep an existing tenant id of `default`. An empty tenant id is still rejected.
- SQL Server view and checkpoint stores are in the package `LawnDart.Projections.Lightweight.SqlServer`. Add that package. `SqlViewStore`, `SqlCheckpointStore`, `AddSqlProjectionStores`, and `InitializeSqlProjectionStoresAsync` are in the namespace `LawnDart.Projections.Lightweight.SqlServer`. Add `using LawnDart.Projections.Lightweight.SqlServer`. `LawnDart.Projections.Lightweight` no longer depends on `Microsoft.Data.SqlClient`.

### Added

- A save that commits in this process wakes same-process subscriptions, live projection views, and the outbox processor. Each reader takes a ticket before it reads, then waits on that ticket or its existing poll interval, whichever comes first, and reads again from its own checkpoint. A save that commits after the ticket wakes the reader at once. The signal is the newest global position and carries no events. A missed signal still delivers on the poll timer. On the in-memory store that timer is `SubscriptionPollInterval` (default 1 second). Saves from another process are still picked up on the poll timer. Code outside the store can raise the same signal.

### Changed

- Docs call Eventhesis a separate product that can output slice JSON you use as a spec for building with LawnDart. Putting `Id` and `Timestamp` first is a style tip. Declaration order is not a runtime rule.
- The shared backend contract tests cover the event log, event store, dynamic consistency boundary, tags, stream registry, subscriptions, read horizon, snapshots, outbox, inbox, checkpoints, and views. In-memory and SQL Server both run the tests they can. A test that needs a hook the backend does not have is skipped with a reason.
- `default` in any casing is a reserved tenant id. A caller-supplied tenant id of `default`, `Default`, or `DEFAULT` is rejected. Set `LawnDartOptions.ValidateNames` to false to keep an existing tenant id of `default`. An empty tenant id is still rejected. When `RequireTenantId` is true and no tenant id is given, the save throws.
- Guides and API comments call the catalog token an event name, and `CodecId` the payload-format number. A typed read that cannot decode a stored record says what it refuses, in plain language.

## [0.6.0-alpha.3] — 2026-10-18

Upgrade SQL Server before the new process reads. The steps are in [Upgrading from 0.6.0-alpha.2](docs/packages/eventsourcing-sqlserver.md#upgrading-from-060-alpha2). Set `LawnDartOptions.ValidateNames` to false before `AddBoundedContext` to keep a bounded context name or tenant id that does not match `LawnDartNames.Pattern`.

### Breaking

- Bounded context names and tenant ids must match `LawnDartNames.Pattern` (`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`). A name starts with a letter or digit, then letters, digits, `.`, `_`, or `-`, and is at most 63 characters. The spelling is kept. A second context whose name differs only by case is still rejected. `Default` is rejected. The default context is `default`. Set `LawnDartOptions.ValidateNames` to false before `AddBoundedContext` to keep an existing name. A colon in a tenant id is still rejected, because a stream id is split on every colon. An invalid `X-Tenant-Id` header returns 400.
- A custom `IInboxStore` must implement `TryClaimAsync` and `ReleaseClaimAsync`. `TryClaimAsync` returns true only when this caller holds the key. Return false when the key is already processed inside the deduplication window, or a live claim is held. `ReleaseClaimAsync` drops a claim that has not been marked processed and leaves a processed key in place. Hosted reactors and processors claim before the handler and release when the handler throws.
- `Events.EventId` is unique. An append whose metadata `EventId` is already in the log throws `EventAlreadyAppendedException`, and the stored row stays. This applies to an app that sets `EventId` in metadata and sends that same id on a later append. Give each new event its own id. Catch `EventAlreadyAppendedException` when the call is a retry of an append that already landed. The in-memory log rejects that reuse the same way. A format 2 or 3 table has no `EventId` column, so the upgrade fills each row with `NEWID()` and the unique index succeeds. If the column is already there and two rows share an id, creating `UX_EventId` fails and the stamp stays put. Give those rows distinct ids and call `InitializeSchemaAsync` again.
- `InitializeSchemaAsync` upgrades a SQL Server events table stamped `LawnDart_SchemaFormat` 2 or 3 to format 4. It adds `Events.RowVersion`, creates `AppendFence`, sets `EventTags.Tag` to the `Latin1_General_100_BIN2` collation, and adds unique `Events.EventId`. The same call on the outbox writer upgrades outbox format 1 to 2 by adding nullable `ClaimedUntil`. `InitializeSqlInboxStoreAsync` upgrades inbox format 1 to 2 by making `ProcessedAt` nullable and adding nullable `ClaimedAt`. Existing rows stay. A missing stamp, or any stamp other than 2, 3, or 4 on events, 1 or 2 on the outbox, and 1 or 2 on the inbox, is a drop and recreate. The stamp does not move backward.
- SQL Server reads use a commit horizon. `GetCurrentSequenceAsync` returns the highest sequence below `MIN_ACTIVE_ROWVERSION()`. One unfinished write may hold that horizon for `ReadHorizonGracePeriod` (default 30 seconds). After that, readers move to committed rows past it and log a warning. `Timeout.InfiniteTimeSpan` waits until the write ends. A row that commits below that head logs an error in the process that moved the horizon.

### Added

- `ITagFormat` builds a tag from a type and an id. `AddLawnDart` registers `LowercaseTagFormat` (lowercase type, `Guid` format `D`, other ids unchanged) unless a format is already registered. Override it with `AddTagFormat<T>()` or `WithTagFormat<T>()`. `TagProvider` is an optional base class that takes that format. `WarnOnUnknownTag` (default true) logs once per tag type in that context when a queried tag does not round-trip through the format. Different ids of the same type share one warning. After 128 tag types, further unknown tag warnings in that context are suppressed. The check does not throw and does not change which events match. `FormatAggregateFallbackTag` (default false) formats the aggregate fallback tag. Leave it false on a store that already has `{Type}:{id}` tags. Those events stay under the old tag.
- `QueryResult.TypesResolved` and `EventLogQueryResult.TypesResolved` are false when a queried event type is still unknown after the store reloads its type map. An empty result then does not mean the range was empty.
- `SqlServerEventStoreOptions.ReadHorizonGracePeriod` (default 30 seconds) is how long one unfinished SQL Server write may hold the read horizon. After that, readers move to committed rows past it and log a warning. `Timeout.InfiniteTimeSpan` waits until the write ends. A row that commits below that head logs an error in the process that moved the horizon.
- `OutboxClaimLease` (default 30 seconds) is how long an outbox claim hides a row. `InboxClaimLease` (default 30 seconds) is how long an inbox claim blocks a second delivery. `TimeSpan.Zero` expires either claim at once.

### Changed

- Tag equality is ordinal. `Customer:ABC` and `customer:abc` are different tags on SQL Server reads, SQL Server subscriptions, and the in-memory store.
- `IEventStore` and `IEventLog` have the same members as 0.6.0-alpha.2. `QueryResult.TypesResolved` and `EventLogQueryResult.TypesResolved` are new properties and default to true, so an existing constructor call still compiles.
- [Tagging](docs/TAGGING.md) treats the context `ITagProvider` as the source of tag strings. Load queries, `Emit`, and append fences use that same lowercase format (`student:{id}`). A different spelling does not match.

### Fixed

- A SQL Server stream id prefix treats `_`, `%`, and `[` as literal characters. `acme_west:` does not match `acmeXwest:`. The query uses the `StreamId` column collation, so the prefix can seek that index. A new database is case-insensitive. The in-memory store matches with ordinal ignore-case.
- A SQL Server stream id longer than 255 characters throws `ArgumentException` before the write. `Events.StreamId`, `Streams.StreamId`, and `EventSnapshots.StreamId` are `NVARCHAR(255)`.
- An append is not retried after `Commit` has been attempted. If that commit's reply is lost, the store reads the event ids back and returns the append that landed. Replaying the same event id fails with `EventAlreadyAppendedException` instead of writing a second row. A typed append uses `IEvent.Id` as that id, so one `EventMetadata` object can be passed to several appends.
- Outbox message ids are the event ids. A processor claims outbox rows (`UPDLOCK, READPAST`) for 30 seconds, so two processors do not publish the same row. A failed publish releases the claim.
- Inbox delivery claims the message key before the handler runs. A second delivery does not run the handler while that claim is live. A failed handler releases the claim.
- A failed event subscription completes with `EventSubscriptionFailedException`. Cancellation and dispose still complete the channel without an error.
- A projection joins the shared event pipe under the pipe lock, so an event fanned out during the join is not skipped.
- SQL Server subscription catch-up reads one page at a time and moves its scan cursor to the highest position it examined.
- A SQL Server subscription or projection reads an earlier append that commits after a later sequence, including when `READ_COMMITTED_SNAPSHOT` is on, while that append stays inside `ReadHorizonGracePeriod`.
- A SQL Server reader in another process sees an event type that was first written elsewhere. It reloads `EventTypes` on a cache miss. A projection does not checkpoint past a type that is still unknown.

## [0.6.0-alpha.2] — 2026-10-16

### Breaking

- Queue names changed. On RabbitMQ, drain the old queues, delete the old exchanges and queues, or use a fresh vhost. A broker will not change an existing exchange type. On Amazon SQS, delete the old `{topic}--{subscription}` queues. SQS will not rename a queue.

### Changed

- RabbitMQ uses one quorum queue per subscription. The queue name is the subscription name. It is bound to each event type that subscription handles. Set `QueueLayout` to `EventType` for one queue per event type, named `{subscription}.{event-type}`.
- RabbitMQ retry and dead-letter resources are one set per subscription in both layouts. The names are `{subscription}.retry` (a direct exchange and a quorum queue), `{subscription}.delay`, `{subscription}.dlx`, and `{subscription}.dlq`. The delay and dead-letter exchanges route by event type name.
- Amazon SQS queue names start with the subscription. The queue is `{subscription}--{topic}`. The dead-letter queue is `{subscription}--{topic}--dlq`. Both must fit in 80 characters. A longer name is rejected. There is no shortened or hashed form.

## [0.6.0-alpha.1] — 2026-10-15

### Breaking

- Add `[EventTypeName("token")]` to every event a reactor, an event processor, or the in-memory transport publishes or subscribes. In 0.5.0-alpha.1 the class name was used in logs and the CLR type was the subscription key, and the attribute was not required. A missing name fails when that consumer starts. The class name is not the message type.
- A failed in-memory handler runs again until `MaxDeliveryAttempts` (default 5). In 0.5.0-alpha.1 the handler ran once and the exception was thrown on that call. Dead-letter handling is off unless `DeadLetterFailedMessages` is set. After the last attempt the exception is thrown, so the outbox keeps the row. Set `MaxDeliveryAttempts` to 1 for a single try.

### Added

#### LawnDart.Messaging

- `MessageTypeNames.Require` reads `[EventTypeName]`. Host startup checks that token, transport settings, and topology setup.
- `ITopicMapper` maps a declared event type to a topic or exchange. The default is one topic per `[EventTypeName]`. `GroupTopicMapper`, `PrefixTopicMapper`, `SharedTopicMapper`, and `DelegateTopicMapper` group types. `SanitizingTopicMapper` replaces each period with a hyphen and leaves the default mapper unchanged. Startup rejects two declared names that sanitize to the same topic unless the inner mapper already sends them to one topic.
- `MessagingJson.Options` is the JSON setting for broker message bodies and the outbox publisher. Property names are PascalCase. Reads ignore case. Null values are omitted. That matches the outbox publisher settings in 0.5.0-alpha.1.
- `MessageHeaders` names the envelope headers: message id, type, correlation, causation, user, tenant, enqueued time, a `header.` prefix for custom headers, the outbox headers (`outbox.streamId`, `outbox.sequencePosition`, `outbox.eventType`), and the W3C trace headers. The three outbox header strings are the ones the 0.5.0-alpha.1 publisher wrote.
- `MessageTransportOptions` sets the subscription name, concurrency, prefetch, max delivery attempts, and dead-letter handling. `DeadLetterFailedMessages` defaults to false.
- `IMessageTopologySetup` creates topics, queues, or exchanges when `SetupTopology` is true. Startup skips that step when the flag is false.
- `IMessagePreHandler` runs before the message handler on each delivery attempt. Register it with `AddMessagePreHandler`.
- `LawnDart.Messaging.Testing` ships `MessageTransportConformanceTests`. The in-memory transport passes that suite.

#### LawnDart.Messaging.InMemory

- Publish and subscribe require `[EventTypeName]`. A failed handler runs until `MaxDeliveryAttempts` (default 5). With dead-letter handling left off, the last failure is thrown. Set `DeadLetterFailedMessages` to park the message on `IDeadLetterQueue` and return.

#### LawnDart.Messaging.RabbitMQ

- `LawnDart.Messaging.RabbitMQ` publishes and consumes through RabbitMQ (`AddRabbitMqTransport`). It first ships in 0.6.0-alpha.1. The exchange comes from `ITopicMapper`. The routing key is the declared event type name. Publish declares the exchange. Consumers declare their queue.
- `UnroutablePublish` defaults to `Warn`. A publish that reaches no queue logs a warning and returns. It is not recorded on `messaging.publish.duration`. It increments `messaging.publish.errors` with `messaging.publish.outcome` set to `unroutable`. Set `UnroutablePublish` to `Throw` when the caller should see that failure.
- A failed delivery waits on a retry queue (`RetryDelay`, default 5 seconds) and then returns to the same queue. A cancelled delivery uses that wait and does not increment the attempt count. With dead-letter handling on, a message that used its attempts is nacked to a dead-letter exchange. With it off, the message stays on the queue and the wait still applies.
- `OperationTimeout` (default 30 seconds) is how long to wait for automatic connection recovery before opening a new connection. The subscription concurrency limiter is disposed with the transport.

#### LawnDart.Messaging.AzureServiceBus

- `LawnDart.Messaging.AzureServiceBus` publishes and consumes through Azure Service Bus (`AddAzureServiceBusTransport`). It first ships in 0.6.0-alpha.1. The topic comes from `ITopicMapper`. Each subscriber has one subscription on that topic. Connect with a connection string, a fully qualified namespace and a token credential, or a `ServiceBusClient` already registered.
- Publish sets `lawndart_target` to `*` and `lawndart_type` to the declared event type. A message with no `lawndart_target` reaches every subscription that accepts the event type. A message with no `lawndart_type` still matches the type clause. A retry sets `lawndart_target` to the subscription that failed and keeps `lawndart_type`.
- The subscription rule lists every recorded type for that topic before those handlers subscribe. A later subscribe does not drop a type. A filter change, including the move from a rule that only tests `lawndart_target`, creates the new rule before it deletes the old one. Two hosts that update the same rule both complete: a rule name that already exists is read again, and a rule with the same filter counts as success. `ReplaceRouteRule` rebuilds the rule from the types this process recorded. The default leaves types from an earlier deploy on the rule.
- A recorded type with no handler waits `RetryDelay` up to `MaxHandlerDeferrals` (default 12). Each wait increments `messaging.deferred`. From the third wait a warning is written. After the cap, the wait counts as a failed attempt.
- A failed delivery is scheduled again after `RetryDelay` (default 5 seconds) for that subscription only. A cancelled delivery uses that same wait. `BrokerMaxDeliveryCount` defaults to 2000. The emulator accepts 1 to 10. With dead-letter handling on, a message that used its attempts is dead-lettered on the subscription. With it off, the message is scheduled again and the wait still applies.
- `Dispose()` blocks the caller for up to `OperationTimeout` (default 30 seconds) and then returns. The connector logs a warning when that wait ends first. Prefer `DisposeAsync`.

#### LawnDart.Messaging.AwsSqs

- `LawnDart.Messaging.AwsSqs` publishes and consumes through Amazon SQS (`AddAwsSqsTransport`). It first ships in 0.6.0-alpha.1. SNS fans each topic out to one queue per subscription, with raw message delivery. The topic comes from `ITopicMapper`. Connect with `Region` or `ServiceUrl` (LocalStack). Credentials come from the AWS SDK default chain. An `IAmazonSQS` and `IAmazonSimpleNotificationService` already registered are kept. FIFO queues and topics are not supported.
- Publish sets `lawndart_type`. User, tenant, and custom headers travel in `lawndart-headers`, because SNS and SQS allow 10 message attributes. When several types share a topic, the filter is `lawndart_type` equal to a handled type, or the attribute missing. The filter only grows unless `ReplaceRouteRule` is set. After a write, the transport waits a short jittered delay and reads the policy again, three times, and merges a stale write. A write that lands after that last read waits for the next subscribe. An SNS subscribe that returns `InvalidParameter` and "already exists" is read again and merged. The filter allows 150 values. The missing-attribute match counts as one.
- A failed delivery is sent again to that queue after `RetryDelay` (default 5 seconds, at most 15 minutes). It is not published to the topic. A cancelled delivery uses that wait and does not count an attempt. While a handler runs, visibility is extended at about half of `VisibilityTimeout` (default 30 seconds), up to 240 times. A failed extension is logged.
- With dead-letter handling on, a message that used its attempts is sent to `{topic}--{subscription}--dlq`, and the source queue has an SQS redrive policy. `MaxReceiveCount` defaults to 10. With dead-letter handling off, LawnDart does not set a redrive policy. A crash returns the message after the visibility timeout and the attempt count is unchanged.
- An SQS or SNS message over 256 KiB, including attributes, is rejected before publish and before a retry send. A retry that cannot be sent is dead-lettered when that handling is on, and deleted when it is off.

#### Outbox

- The `LawnDart.Outbox` meter counts `outbox.publish.failures` and `outbox.deadlettered` on the outbox processor's warning paths.

#### Telemetry

- The `LawnDart.Messaging` meter counts `messaging.publish.duration`, `messaging.consume.duration`, `messaging.publish.errors`, `messaging.inbox.duplicates`, `messaging.delivery.retries`, `messaging.deferred`, and `messaging.deadlettered`. A retry is a debug log and a `messaging.retry` span event. A deferral is a `messaging.deferred` span event. A dead-letter is a warning and a `messaging.deadlettered` span event, and the span status is Error. An unroutable publish increments `messaging.publish.errors` with `messaging.publish.outcome` set to `unroutable` and is not recorded on `messaging.publish.duration`.

### Changed

- Reactor and event processor logs, inbox duplicate counts, and handler spans use the declared event type name. In 0.5.0-alpha.1 they used the class name.
- The build kit (`skills/BUILD_KIT.md`) targets LawnDart 0.6, matching the 0.6.0-alpha.1 packages.

### Documentation

- The README shows the in-process, RabbitMQ, Azure Service Bus, and Amazon SQS transports, how to install them, and a minimal registration for each. Pick one registration. RabbitMQ, Azure Service Bus, and Amazon SQS first ship in 0.6.0-alpha.1.
- [Message transports](docs/MESSAGE_TRANSPORTS.md) covers retry delay, attempt counting, dead-letter handling, topic mapping, and the broker limits.
- [Telemetry](docs/TELEMETRY.md) lists every LawnDart meter and activity source.
- [Custom message transport](docs/CUSTOM_MESSAGE_TRANSPORT.md) explains how to write a transport against the shared pieces.
- The outbox guide registers the publisher with the in-process transport, RabbitMQ, Azure Service Bus, and Amazon SQS. `DeadLetterFailedMessages` stays off unless you set it. A publish that throws leaves the outbox row in place.
- The SQL inbox readme names those transports. The inbox package replaces the inbox store. The transport you registered stays.

## [0.5.0-alpha.1] — 2026-10-06

### Fixed

- Reactor and task-processor command dispatch creates a DI scope, so a scoped `AuthorizationService` resolves when the handler loads an aggregate.
- The Shop demo loads cancel and ship on the buyer's order stream. A seller can cancel only their own orders. Paying an order requires `Order.Pay`. A repeated order id fails before stock is reserved.
- A SQL Server append that loses the events-table key throws `ConcurrencyException` with the expected and actual stream version.
- Hosted reactor and processor inbox keys hash the consumer type's full name, so a long or generic name stays within the SQL Server 256-character limit. A message marked with the previous key can run once more.
- The InMemory subscriptions demo resets a checkpoint outside `[0, head)`, including one left by a partial write, so a second run finishes.
- Projection telemetry docs name the read-source tags `memory` and `sql`.
- Two reactors or processors that handle the same message no longer share one inbox entry; each consumer deduplicates on its own.
- Projection store API docs name the shipped stores (InMemory and SQL Server) only.
- Configured schema, table, constraint, and index names that contain `]` or `'` are escaped before they are placed in SQL text.
- The tenant prefix check on a stream id uses an ordinal comparison.
- The BDD testing guide links to `build-kit/library-dcb-slice.json`.

### Added

- An ECommerce console demo (`demos/LawnDart.Demo.ECommerce`) runs cart and order scenarios as aggregates, DCB, a hybrid of both, and EDA, on InMemory or SQL Server. The SQL path also runs the transactional outbox, including a dead-letter reset.
- A Shop demo (`demos/LawnDart.Demo.Shop`): Blazor UI over aggregates, DCB stock reservation, a reactor chain, permissions, and tenant-prefixed streams. InMemory by default, SQL Server by launch profile.
- A SQL Server subscriptions demo (`demos/LawnDart.Demo.SqlServerSubscriptions`) shows catch-up then poll-backed live delivery.
- An InMemory subscriptions demo (`demos/LawnDart.Demo.InMemorySubscriptions`) shows catch-up then live delivery with a client checkpoint.
- A multi-context demo (`demos/LawnDart.Demo.MultiContextInMemory`) runs two bounded contexts with separate InMemory stores in one host.
- Dead-lettered outbox messages can be reset with `ResetDeadLetteredAsync` or `ResetAllDeadLetteredAsync`. The outbox processor republishes them on its next poll with the same message id.
- `LawnDart.Messaging.SqlServer` stores inbox deduplication in SQL Server (`AddSqlInboxStore`), so duplicate deliveries are dropped across restarts and instances.
- `AGENTS.md` is the instruction file for coding agents in this repository. A cloud agent commit includes `Signed-off-by: Cursor Agent <cursoragent@cursor.com>`.
- [Metadata](docs/METADATA.md) covers `MessageContext`, `CommandMetadata`, `EventMetadata`, and what the default provider fills.
- A DCB Library sample (`samples/Library.Dcb.Domain`) implements the same book intents as a `BookLoan` entity that spans book and member. Input is `build-kit/library-dcb-slice.json`.
- The build kit has `lawndart-testing` and `lawndart-reactions` skills.
- Academy WebApi issues a demo JWT at `POST /token`.

### Changed

- The outbox guide keys `IOutboxWriter` by bounded-context name. A dead-letter reset runs the consumer again after the inbox window expires.
- Academy WebApi's SqlServer profile stores projection views in SQL Server, writes the transactional outbox, and creates event-store, outbox, and view tables at startup.
- Academy's SqlServer profile creates event-store and outbox tables at startup and publishes through the transactional outbox.
- `Library.Host` `AddSqlLibrary` registers SQL projection stores, the loan-notice reactor, a SQL inbox, and the outbox. `InitializeSqlLibraryAsync` creates those tables.
- `IOutboxWriter` has two new members. Custom outbox writers must implement them.
- README and Why say the event store, views, and checkpoints swap to SQL Server by DI. Message transport and inbox stay in-process. When the outbox is on, SQL Server rows commit with events.
- The CES matrix and glossary describe the four unhosted cells as planned for a later release, with no public type until a host exists.
- Academy demo tokens expire after 8 hours and are checked for issuer, audience, and lifetime. The built-in signing key is used only in Development.
- Library awaits resume without capturing the caller's synchronization context.
- The skill kit frozen surface lives on `lawndart-host-setup`. Aggregate load is `GetOrCreateAsync`. DCB load is `GetOrCreateEntityAsync`. Projection scopes include `[MultiStreamProjection]`.
- A store-backed second bounded context in `Library.Host` registers `WithEventTypes`.
- [BDD testing](docs/testing/BDD_TESTING.md) excerpts `DcbSpec` from the Library DCB sample.
- Command HTTP endpoints map success to 202, failed authorization to 403, `ConcurrencyException` to 409, and `DomainException` to 422.
- Guides lead with what LawnDart is and what to do.

## [0.4.0-alpha.7] — 2026-09-17

### Changed

- Log frames (`AppendEvent`, `RecordedEvent`, `OutboxMessage`) store a codec id (`byte`) instead of a MIME string. `IEventSerializer.ContentType` is still the plugin name; `EventCodec` maps it to the id. `0` is rejected; `255` is opaque and does not hydrate on the typed path.
- The event-type catalog is per bounded context and `WithEventTypes` is required. There is no process-wide resolver and no FullName / simple-name read alias. An unknown token fails closed.
- The core `LawnDart` package no longer depends on MemoryPack. `EventMetadata` is System.Text.Json only. MemoryPack remains an optional session codec (`EventCodec` id `2`) for event payloads.
- SQL Server stores one `EventData` (`VARBINARY`) payload and a `CodecId` (`TINYINT`). `SchemaVersion` and `CodecId` have no column defaults. Opening an older events table throws and names drop-and-recreate. A `{table}_Readable` view is created for SSMS.
- SQL Server DCB queries and append-condition fences always use the `EventTags` table. `UseEventTagsTable` is gone. `Events.Tags` is a read projection only.
- SQL Server stores the family token in an `EventTypes` lookup and `Events.EventTypeId`. Type filters are integer seeks. Opening a format-1 events table throws and names drop-and-recreate.
- SQL Server outbox rows store payload bytes and a `CodecId`. `SchemaVersion` and `CodecId` have no column defaults. Opening an older outbox table throws and names drop-and-recreate. `EnableOutbox` writes rows even when no writer is injected.
- Schema changes are a wipe before 1.0. An older events or outbox table is not migrated in place.

### Removed

- `EventTypeNameResolver`, `EventTypeCatalog.Shared`, and the parameterless `EventTypeCatalog` constructor.
- `AppendEvent.ContentType`, `AppendEvent.DefaultContentType`, and MIME string properties on `RecordedEvent` and `OutboxMessage`. `OutboxMessage.Payload` is bytes, not a string.

## [0.4.0-alpha.6] — 2026-09-17

### Added

- `LawnDart.Analyzers` reports event schema versioning mistakes at `dotnet build`: two currents for one token (`LDT001`), a multi-type family with no `current: true` (`LDT002`), and an incomplete upcaster chain (`LDT003`). One-arg `[EventTypeName("token")]` is not a diagnostic. Warmup is still the runtime authority.

## [0.4.0-alpha.5] — 2026-09-16

### Fixed

- CI and release run `Category=Integration|Category=Contract` on the SQL Server, Lightweight, and backend-contract test projects instead of the whole solution, so a passing filtered run is not failed by assemblies with no matching tests.

## [0.4.0-alpha.4] — 2026-09-16

### Added

- `EventTypeCatalog.Materialize` builds a scoped immutable catalog. Typed resolve is family token plus `SchemaVersion`. `[EventTypeName(token, version: n, current: false)]` marks a historical or current schema. One-arg `[EventTypeName("token")]` is still version 1 and implicitly current when it is the only type for that family.
- [Event schema versioning](docs/EVENT_SCHEMA_VERSIONING.md) is the consumer guide: family tokens, first-class `SchemaVersion`, upcast on read, the fail-closed matrix, expand-contract deploy, and how to implement `IEventLog`.
- Typed reads throw dedicated `EventHydrationException` types when the stored version is newer than this process, the family is unknown, a historical type is missing, the content-type does not match, or the payload will not deserialize. Log and raw copy paths still return the frame. Lightweight projections back off five seconds on a stuck sequence; there is no skip override.
- `IEventUpcaster<TTo, TFrom>` and `WithUpcasters` register an upcast chain (v1 → v2 → v3). Warmup fails if any historical version cannot reach current. There is no downcast API.

### Changed

- `WithEventTypes` registers that catalog per bounded context. Two contexts do not share a mutable global catalog. `EventTypeNameResolver` remains a process-wide compatibility wrapper.
- Typed append rejects a historical CLR type for a family the catalog already knows. This process writes only its current type. The log still accepts frames from older binaries.
- Typed append stamps `SchemaVersion` on the log frame from the current CLR type and mirrors it onto `EventMetadata`. Hydrate reads the frame, not the metadata blob. Missing or zero version is `1`.
- Typed hydrate deserializes the stored version's CLR type, then upcasts to this process's current type. A missing hop throws `MissingEventUpcasterException`. The outbox publisher uses the same `EventSession` path. Log and raw copy still return the stored frame.
- The library `Book` aggregate throws `DomainException` for rule violations. Learning-path and testing docs assert `ThenThrows<DomainException>()`.
- Docs no longer mention the removed `IMessage` marker. Commands are `ICommand`; events are `IEvent`.

## [0.4.0-alpha.3] — 2026-09-16

### Added

- Third-party stores implement `IEventLog` (`AppendEvent` in, `RecordedEvent` out). Application code still uses `IEventStore`; that typed session is unchanged.
- The InMemory → SQL contract (`Category=Contract`) also covers the event log: the same payload on append/read, token and tag query without hydrate, and a host that has not registered event CLR types.
- `RawRecordedEvent` implements `IRawEvent` over a recorded frame. A host without the CLR event type appends and copies through `IEventLog`; typed hydrate still fails closed.
- [Why LawnDart](docs/WHY.md) contrasts LawnDart with Marten, Cratis, and Axon, and names the supported scope as a closed set (InMemory, SQL Server, in-process, `net10.0`).
- The library domain (`samples/Library.Domain`) is the canonical reference slice: a `Book` aggregate whose decision state (`BookState`) is not the catalog view (`BookCatalogView`), with three given/when/then specs in `samples/Library.Domain.Tests`.
- `build-kit/library-slice.json` is the canonical demo input. Skills excerpt host and domain snippets from `samples/Library.Domain` and `samples/Library.Host`. CI compiles the slice and runs its GWT tests on every change.
- `skills/BUILD_KIT.md` is the agent entry point for the in-repo build kit. It declares `Targets LawnDart 0.4`; CI fails if MinVer's major.minor is not that pair. Eventhesis is documented as one adapter over a generic slice spec, not as the kit's definition.

### Changed

- `IEventSerializer` now reads and writes `ReadOnlyMemory<byte>` (UTF-8 JSON by default). `EventSession` maps `IEvent` to `AppendEvent` and hydrates `RecordedEvent` so a store does not resolve CLR types.
- InMemory stores recorded events: append serializes, read hydrates a new instance. Mutating an appended event or its metadata is not visible on the next read.
- SQL Server stores recorded events: new payloads go to `EventPayload` (`VARBINARY`); old `EventData` (`NVARCHAR`) rows still read. `SchemaVersion` is a first-class column. The backend does not resolve CLR types.
- `IEventStore` is a typed adapter over `IEventLog`. InMemory and SQL Server implement the log; subscriptions hydrate in the adapter. A fail-closed hydrate does not advance the typed cursor.
- `IRawEvent` docs no longer name `OpaqueEvent` or `UnknownEvent` as LawnDart types. The catalog still rejects `IRawEvent`.
- SQL Server outbox rows copy the appended frame (family token, UTF-8 payload/metadata text, `SchemaVersion`, and `ContentType`) in the same transaction, instead of re-serializing a CLR event. Existing outbox rows without those columns read as version `1` and `application/json`. The message-transport publisher resolves types through the event catalog (token + schema version), not a private token-only map.
- Skills, Quickstart, and the package map state that InMemory serializes on append and that third-party stores implement `IEventLog`. `IEventSerializer` is documented as `ReadOnlyMemory<byte>`.
- The README, docs site landing, Start Here, and Overview lead with the same sentence: LawnDart is the .NET runtime that event-modeled systems compile into. The README quick start is a complete Counter — command, event, aggregate, `HandleCommandAsync`, and read-back — compiled in CI.
- Learning-path steps 2–4 teach the library `Book` domain. Snippets are excerpted from `samples/Library.Domain`. The README and Quickstart keep the short Counter.
- Snapshot docs state replace-in-place retention (one row per stream, no scavenge) and the strategy constructors from source. A dropped or bad snapshot costs replay time; the log stays the source of truth.
- The command–event–state matrix lives on [CES matrix](docs/CES_MATRIX.md) as the shapes an event model compiles into. Five cells are hosted; four are roadmap with no public type. Other pages link there instead of repeating the grid.
- Academy’s README points at the CES matrix instead of implying every From×To cell is hosted.

### Fixed

- `AddLawnDart` registers a default `AmbientTenantContextProvider` so `UseInMemory()` can resolve `IAggregateRepository` without a hand-written tenant registration. `RequireTenantId = false` hosts copy-paste as documented.

## [0.4.0-alpha.2] — 2026-09-14

### Added

- `UseInMemory()` registers an in-memory `ISnapshotStore` / `IDcbSnapshotStore` (replace-in-place) and an `IOutboxWriter`. In-memory hosts can persist snapshots and run the outbox processor without SQL Server.
- Third-party stores construct repositories through `EventSourcingRepositories` and register the write path with `AddSnapshotWriteInfrastructure`. The public constructors omit the snapshot write queue; without this seam a custom `Use*` host silently stops writing snapshots.
- A contract test runs the same application against InMemory and SQL Server (Testcontainers). The swap covers command dispatch, event persistence, aggregate reload, projection materialisation, and read-back. Outbox and subscriptions are not part of that guarantee.

### Changed

- Snapshot writes capture committed state on the calling thread, then enqueue wait-free onto a bounded channel (`DropOldest`). A hosted consumer performs the store I/O. A full channel drops the oldest pending write, increments a drop counter, and degrades `SnapshotWriteHealthCheck`. Store failures increment a failure metric and the same health check. `HandleCommandAsync` never waits on snapshot I/O and is not failed by a drop or a store error.

## [0.4.0-alpha.1] — 2026-09-14

### Added

- `WithCommandHandlers<TMarker>()` and `WithEventTypes<TMarker>()` register from the marker type's assembly. Prefer these over `Assembly.GetCallingAssembly()` fallbacks.

### Changed

- `AddLawnDartHttpCommands` no longer registers `ICommandHandler<T>`. Register handlers once with `WithCommandHandlers<TMarker>()` (or `WithCommandHandlers`) on the bounded context. The `"default"` context still resolves handlers unkeyed via an alias built through `ContextServiceProvider`. A scan that finds no handlers or event types fails at warmup and names the scanned assembly (and `Assembly.GetCallingAssembly()` when that fallback chose it).
- `AggregateSpec` assertion failures throw `BddSpecAssertionException` instead of `InvalidOperationException`. The new type does not derive from `InvalidOperationException`, so `Assert.ThrowsAsync<InvalidOperationException>(() => spec.RunAsync())` no longer passes when the spec itself failed. `ThenThrows<T>()` now matches derived exception types, not only an exact type.
- Messaging and authorization docs name only implementations shipped in these packages: `InMemoryMessageTransport` and `AddHttpAuthorizationContext`. Storage docs name `UseInMemory` and `UseSqlServer` only.

### Removed

- The four unhosted CES interfaces (`ICommandDelegator`, `IDownstreamActivity`, `IEventGenerator`, `IStateTransformer`). They had no host and no implementation. `IProjector<TState>` stays.
- `ProjectionDescriptor.UseRedis`. It was never read and defaulted to `true`.
- `MessagingOptions.ServiceBusConnectionString`. It was never read; these packages do not ship a Service Bus transport.

### Fixed

- Docs site links no longer point at the repo-root README (that file is not part of the DocFX build).

## [0.3.0-alpha.1] — 2026-09-10

### Added

- `EventMetadata` and `CommandMetadata` carry `TraceId` and `SpanId` (W3C hex).
- `AmbientMessageContext` publishes inbound `MessageContext` for metadata capture without changing `ICommandHandler<T>`.
- `MessageTrace` starts or continues a W3C Activity from `traceparent` and writes cheap `correlation_id` / `messaging.message_id` tags.

### Changed

- `CaptureCommandMetadata` reads `Activity.Current` and ambient `MessageContext` for correlation, causation, tenant, user, and W3C trace ids. Without a trace it still mints a correlation id.
- `ContextAwareCommandDispatcher` uses the inbound `MessageContext` (ambient publish + `traceparent` Activity) before `HandleAsync`.
- `HandleCommandAsync` sets envelope `CausationId` to the command id when the caller left it unset. Correlation is not copied from causation.
- HTTP command endpoints continue `traceparent`, publish ambient `MessageContext`, and may assign `ICommand.Id` from `Idempotency-Key` when the body omits it.
- HTTP authorization context uses the W3C trace id, not `HttpContext.TraceIdentifier`, as `CorrelationId`.
- Messaging hosts start Activity from inbound `traceparent` before handle. Outbox publish copies `TraceId` / `SpanId` / `traceparent` onto `MessageContext.Headers`. `CreateChild()` overlays the current span's trace headers.
- Event enrichment copies `IEvent.Timestamp` onto the envelope. It no longer overwrites business time with `DateTime.UtcNow`. `CommitTimestamp` is still set only at append. Time-travel (`toTimestamp`) uses envelope `Timestamp`.
- Event type names stored on append are the `[EventTypeName]` catalog token. Register types with `WithEventTypes`. CLR `FullName` is not written; older FullName rows still resolve as a read alias. Missing attributes and duplicate tokens fail at warmup.
- Sellistix.Patterns consumes these packages at `0.2.0-alpha.2` instead of shipping a second Core.

### Removed

- The empty `IMessage` marker. `ICommand` and `IEvent` no longer inherit it. `IMessageTransport` is unchanged.

## [0.2.0-alpha.2] — 2026-09-08

### Added

- Core ships `PublicAPI.Shipped.txt`. Adding or removing a public member fails the build unless that file is updated. Store implementers must keep the documented `IEventStore`, `IStreamRegistry`, and `IEventStoreSubscriptions` methods.
- `IAggregateRepository` can load and create aggregates by `string streamId`. Guid overloads still build `{type}:{id}` / `{tenant}:{type}:{id}`.
- Aggregates and DCB entities can declare closed `Handle(TCommand)` methods. `HandleCommandAsync` uses those when `HandleAsync<TCommand>` is not overridden. An unknown command throws.

### Changed

- Academy aggregates use closed `Handle(TCommand)`. Quickstart copy-paste is `Handle(CreateCounterCommand …)` and reads state back.
- Host grammar docs and skills state the frozen surface: `ICommandHandler<T>` for HTTP/jobs, closed `Handle(TCommand)` on aggregates, `string streamId` loads, `ProjectionBase` + `IMultiStreamEntityResolver`, and `UseInMemory` / `UseSqlServer`.
- `ICommandDispatcher` now ships in Core (same `LawnDart.Messaging` namespace). EventSourcing registers `ContextAwareCommandDispatcher` from `UseInMemory` / `UseSqlServer` / `WithCommandHandlers` and no longer needs the Messaging package for that registration.
- Aggregate and DCB store-internal mutators (`SetStreamId`, `SetVersion`, `SetCommittedVersion`, `SetTags`, `SetConsistencyTags`, `SetConsistencyMarker`, `ClearPendingEvents`) are no longer public. Load through `GetOrCreateAsync` / `HandleCommandAsync`.
- Packages install from nuget.org with `--prerelease` until a stable version exists.
- CI and release run `Category=Integration` (SQL Server Testcontainers) after unit tests.
- Docs mark the CES matrix: five hosted cells (✅) and four planned interfaces (🔧) with no host.
- Docs table the intentional verb differences (`Apply` vs `Emit`, `Handle` vs `HandleCommandAsync`, `IReactor` vs `IDcbReactor`). These names stay.
- The four planned CES cells (`ICommandDelegator`, `IDownstreamActivity`, `IEventGenerator`, `IStateTransformer`) are experimental (`LAWNDART002`). There is no host; implementing them does not register or run them.
- `IProjector<TState>` is experimental (`LAWNDART001`) and is not the authoring API. Author `ProjectionBase<TView>` plus attributes; multi-stream views implement `IMultiStreamEntityResolver`.
- Projection docs: Lightweight authors `ProjectionBase`; `IProjector` is an optional unused stub. Eventhesis is described as slice JSON, not generated LawnDart types.

### Deprecated

- `HandleAsync<TCommand>` on aggregates and DCB entities. Declare `Handle(TCommand)` instead.

## [0.1.0-alpha.1] — 2026-09-07

### Removed

- Empty public type `LawnDartEventSourcingExtensions`. Registration stays on
  `AddBoundedContext(...).UseInMemory()` / `UseSqlServer()`.
- Unused `eventTypes` parameter on `BddTestContext.CreateInMemory`. The in-memory
  store does not need an event-type catalog.
- Unused `LawnDartOptions.AzureStorageConnection`. Nothing in the library read it.

### Added

- Monthly Dependabot updates for GitHub Actions, a `CODEOWNERS` file, and
  DocFX deploy to GitHub Pages on pushes to `main`. CodeQL is not enabled.
- `.gitattributes` normalizes text to LF in the repository and treats PNG
  and other image assets as binary.
- Quickstart and the first-aggregate learning-path step now run a command
  end-to-end and read the state back.
- Learning-path steps for DCB, reactions, and EDA testing include
  self-contained samples instead of pointers only.
- Package map at `docs/packages/README.md`, with a landing page per package.

### Fixed

- DocFX navbar uses a 64px transparent logo constrained to 32px instead of
  the 512px cream-backed mark, which overflowed the header.
- Doc samples for `IReactor.ReactAsync` and aggregate/DCB `HandleAsync` match
  the current signatures (`MessageContext`, `CancellationToken`).

### Changed

- Copyright and package author metadata name the legal entity as Sellistix LLC.
- `ContextAwareCommandDispatcher` docs now describe automatic registration
  via `UseInMemory` / `UseSqlServer` / `WithCommandHandlers`. There is no
  `AddContextAwareCommandDispatcher` method.
- Package versions are derived from git tags via MinVer. A tag of the form
  `v0.1.0-alpha.1` now produces packages at that version; untagged commits
  produce a deterministic prerelease suffix.
- The Academy console demo resolves `IEventStore`, `IAggregateRepository`, and
  `IDcbRepository` through the library's unkeyed `"default"` aliases instead of
  hand-written keyed bridges.

## [0.1.0-alpha] — 2026-08-20

### Added

- Happy-path packages: `LawnDart`, `LawnDart.EventSourcing`,
  `LawnDart.EventSourcing.SqlServer`, `LawnDart.Projections.Lightweight`,
  `LawnDart.AspNetCore`, `LawnDart.Authorization.AspNetCore`,
  `LawnDart.Messaging`, `LawnDart.Messaging.InMemory`, `LawnDart.Testing`.
- Academy console and WebApi demos on `UseInMemory()` (optional SQL profiles).
- Docs, learning path 1–8, Eventhesis contract, and `lawndart-*` agent skills.
- MIT license, SECURITY.md (`security@sellistix.com`), CI unit filter
  `Category!=Integration`.
