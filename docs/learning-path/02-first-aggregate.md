# Step 2 — First aggregate

**Previous:** [Concepts](01-concepts.md) · **Next:** [Testing](03-testing-your-aggregate.md)

## Types

```csharp
public sealed record CreateCounterCommand(Guid Id, Guid CounterId) : ICommand;
public sealed record IncrementCommand(Guid Id, Guid CounterId) : ICommand;

public sealed record CounterCreated(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;
public sealed record CounterIncremented(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;

public sealed class CounterState : IState
{
    public int Value { get; set; }
}

public sealed class Counter : AggregateRoot<CounterState>
{
    public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
    {
        switch (command)
        {
            case CreateCounterCommand create:
                Apply(new CounterCreated(Guid.NewGuid(), DateTime.UtcNow, create.CounterId));
                break;
            case IncrementCommand increment:
                Apply(new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, increment.CounterId));
                break;
        }

        return Task.CompletedTask;
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        if (@event is CounterCreated) State.Value = 0;
        if (@event is CounterIncremented) State.Value++;
    }
}
```

Keep `Id` and `Timestamp` first on events. That is the Eventhesis contract.

## Host and execute

```csharp
using Microsoft.Extensions.DependencyInjection;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing;

var services = new ServiceCollection();
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default").UseInMemory();
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

Academy domain: `demos/LawnDart.Demo.Academy/Domain`.
