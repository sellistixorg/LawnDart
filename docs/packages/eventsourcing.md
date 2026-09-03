# LawnDart.EventSourcing

Runtime plus the InMemory `IEventStore`.

```csharp
services.AddBoundedContext("default").UseInMemory();
```

Registers keyed `IEventStore`, `IAggregateRepository`, and `IDcbRepository`.
`WithCommandHandlers` scans handler assemblies.
