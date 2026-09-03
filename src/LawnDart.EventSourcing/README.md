# LawnDart.EventSourcing

Event sourcing runtime for LawnDart. Provides aggregate repositories, DCB
repositories, an in-memory event store, JSON serialization, and outbox
processing.

## What's included

- **`AggregateRepository`** — load and save aggregates via any `IEventStore`
- **`DcbRepository`** — Dynamic Consistency Boundary repository
- **`InMemoryEventStore`** — in-process store + portable subscriptions
- **`JsonEventSerializer`** — default `IEventSerializer` using `System.Text.Json`
- **`UseInMemory()`** — documented default backend on `AddBoundedContext`

## Installation

```bash
dotnet add package LawnDart
dotnet add package LawnDart.EventSourcing
```

## Getting started

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default").UseInMemory();
```
