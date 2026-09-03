# Step 2 — First aggregate

**Previous:** [Concepts](01-concepts.md) · **Next:** [Testing](03-testing-your-aggregate.md)

## Types

```csharp
public sealed record IncrementCommand(Guid Id, Guid CounterId) : ICommand;

public sealed record CounterCreated(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;
public sealed record CounterIncremented(Guid Id, DateTime Timestamp, Guid CounterId) : IEvent;

public sealed class CounterState : IState
{
    public int Value { get; set; }
}

public sealed class Counter : AggregateRoot<CounterState>
{
    public override Task HandleAsync<TCommand>(TCommand command)
    {
        if (command is IncrementCommand increment)
            Apply(new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, increment.CounterId));
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

## Host

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
services.AddBoundedContext("default").UseInMemory();
```

Then resolve `IAggregateRepository` (keyed `"default"`, or the unkeyed bridge
Academy registers) and call `HandleCommandAsync`.

Academy domain: `demos/LawnDart.Demo.Academy/Domain`.
