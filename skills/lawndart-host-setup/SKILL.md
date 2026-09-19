---
name: lawndart-host-setup
description: Hub skill for wiring LawnDart in Program.cs. Packages, AddLawnDart, AddBoundedContext order, and which specialized skill to open next. Use when starting a new LawnDart host or choosing registration order.
---

# LawnDart host setup

Use this skill first when wiring a new application on LawnDart packages.

## Frozen surface

1. **App-facing dispatch** is `ICommandHandler<T>` (HTTP, jobs). Register with `WithCommandHandlers<TMarker>()`.
2. **Aggregates / DCB** declare closed `Handle(TCommand)`. `HandleCommandAsync` is persistence + authorization. Do not write the obsolete entity `HandleAsync<TCommand>` switch.
3. **Load:** aggregate `GetOrCreateAsync<T>(id)` when the stream is `{type}:{guid}` (or `{tenant}:{type}:{guid}`); `GetOrCreateAsync<T>(streamId)` otherwise. DCB `GetOrCreateEntityAsync<T>(tags)` then `Emit(event, tags)`.
4. **Projections:** `ProjectionBase<TView>` plus a scope attribute (`[SingleStreamProjection]`, `[GlobalProjection]`, `[DcbProjection]`, `[MultiStreamProjection]`). `[ProjectionEndpoint]` is optional GET mapping. Multi-stream handlers also implement `IMultiStreamEntityResolver`. Do not implement `IProjector<TState>`.
5. **Stores:** `UseInMemory` / `UseSqlServer` on `AddBoundedContext(name)`.

## Packages

| Package | Take it when |
|---|---|
| `LawnDart` | Always. Contracts and host core. |
| `LawnDart.EventSourcing` | Runtime and `UseInMemory()`. |
| `LawnDart.EventSourcing.SqlServer` | Durable store (`UseSqlServer`). |
| `LawnDart.Projections.Lightweight` | Read models. |
| `LawnDart.Messaging` | Reactors or task processors. |
| `LawnDart.Messaging.InMemory` | Choreography without a broker. |
| `LawnDart.AspNetCore` | HTTP POST to a command. |
| `LawnDart.Authorization.AspNetCore` | HTTP claims on those commands. |
| `LawnDart.Testing` | Given / when / then against InMemory. |
| `LawnDart.Analyzers` | Optional `LDT*` schema checks at `dotnet build`. |

## Decision tree

1. **Tests only?** → `lawndart-event-store` → `UseInMemory()` on `"default"`, then `lawndart-testing`.
2. **Durable store?** → `lawndart-event-store` → `UseSqlServer()`. Call `SqlServerEventStore.InitializeSchemaAsync` before the first command. SQL snapshots are opt-in (`WithSnapshots`).
3. **More than one domain store?** → `lawndart-bounded-context`.
4. **Domain types?** → `lawndart-domain-model`. DCB entity → the DCB section there.
5. **GWT proof?** → `lawndart-testing`.
6. **Process / reaction?** → `lawndart-reactions`.
7. **Read models?** → `lawndart-lightweight-projections` after the store, then `lawndart-projection-authoring`. SQL views: `AddSqlProjectionStores` then `InitializeSqlProjectionStoresAsync`.
8. **HTTP?** → `lawndart-aspnet-hosting`.

Bounded context ≠ multi-tenancy. Multiple contexts = separate event stores.

`WithUpcasters` follows `WithEventTypes` when a family has historical types. See `docs/EVENT_SCHEMA_VERSIONING.md`.

## Base registration

Excerpt from `samples/Library.Host/LibraryHost.cs` (`AddInMemoryLibrary`):

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddInMemoryMessaging();
services.AddReactor<LoanNoticeReactor, BookBorrowed>();
services.AddInMemoryProjectionStores("default");
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory();
ctx.WithCommandHandlers<BorrowBookHandler>();
ctx.WithEventTypes<BookAdded>();
ctx.WithProjections(
    [typeof(LibraryCatalogProjection).Assembly],
    opts =>
    {
        opts.PollInterval = TimeSpan.FromMilliseconds(200);
        opts.CheckpointInterval = 100;
    });
```

Canonical demo input: `build-kit/library-slice.json`. DCB alternate: `build-kit/library-dcb-slice.json`. Academy is a runnable host, not the excerpt source.
