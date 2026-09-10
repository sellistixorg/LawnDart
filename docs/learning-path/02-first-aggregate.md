# Step 2 — First aggregate

**Previous:** [Concepts](01-concepts.md) · **Next:** [Testing](03-testing-your-aggregate.md)

## Types

```csharp
public sealed record CreateCounterCommand(Guid Id, Guid CounterId) : ICommand;
public sealed record IncrementCommand(Guid Id, Guid CounterId) : ICommand;

[EventTypeName("counter-created")]
public sealed record CounterCreated(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;

[EventTypeName("counter-incremented")]
public sealed record CounterIncremented(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;

public sealed class CounterState : IState
{
    public int Value { get; set; }
}

public sealed class Counter : AggregateRoot<CounterState>
{
    public void Handle(CreateCounterCommand create) =>
        Apply(new CounterCreated(Guid.NewGuid(), DateTime.UtcNow, create.CounterId));

    public void Handle(IncrementCommand increment) =>
        Apply(new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, increment.CounterId));

    protected override void ApplyEventToState(IEvent @event)
    {
        if (@event is CounterCreated) State.Value = 0;
        if (@event is CounterIncremented) State.Value++;
    }
}
```

Keep `Id` and `Timestamp` first on events if you follow the Eventhesis
field-order convention. Every concrete `IEvent` needs `[EventTypeName]`.

## Host and execute

```csharp
using Microsoft.Extensions.DependencyInjection;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing;
using LawnDart.EventStore;

var services = new ServiceCollection();
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default")
    .UseInMemory()
    .WithEventTypes(typeof(CounterCreated), typeof(CounterIncremented));
var sp = services.BuildServiceProvider();

var repo = sp.GetRequiredService<IAggregateRepository>();
var id = Guid.NewGuid();

var counter = await repo.GetOrCreateAsync<Counter>(id);
await repo.HandleCommandAsync(counter, new CreateCounterCommand(Guid.NewGuid(), id));
await repo.HandleCommandAsync(counter, new IncrementCommand(Guid.NewGuid(), id));

var loaded = await repo.GetAsync<Counter>(id);
Console.WriteLine(loaded!.State.Value); // 1
```

`UseInMemory()` on the `"default"` context registers unkeyed aliases, so
`GetRequiredService<IAggregateRepository>()` resolves without a key. Multi-context
hosts still use `GetRequiredKeyedService<IAggregateRepository>("other")`.

Guid overloads build `{type}:{id}` (or `{tenant}:{type}:{id}`). If the stream
is a custom ID, use `GetOrCreateAsync<T>(streamId)` instead.

Do not load the store yourself. Use `GetAsync` / `GetOrCreateAsync` and
`HandleCommandAsync`. Do not call `SetStreamId`, `SetVersion`,
`SetCommittedVersion`, or `ReplayEvents` from application code.

Aggregates record events with `Apply`. DCB entities use `Emit` (tags).
`Handle(TCommand)` is authoring; `HandleCommandAsync` is the repository.
See [Intentional verb differences](../GLOSSARY.md#intentional-verb-differences).

Academy domain: `demos/LawnDart.Demo.Academy/Domain`.
