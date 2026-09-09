# Quickstart (InMemory)

Zero infrastructure. .NET 10 SDK only.

## 1. Add packages

Packages are on nuget.org as a prerelease. Use `--prerelease` until a
stable version exists:

```bash
dotnet add package LawnDart --prerelease
dotnet add package LawnDart.EventSourcing --prerelease
```

A console host also needs `Microsoft.Extensions.DependencyInjection`.
To work from this repository instead, clone it and add project references
(see the root [README](../README.md)).

## 2. Register the host

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory();
```

`UseInMemory()` registers keyed `IEventStore`, `IAggregateRepository`, and
`IDcbRepository` for `"default"`, plus unkeyed aliases so you can resolve
them without a key.

## 3. Handle a command

Define an `ICommand`, an `IEvent`, and an `AggregateRoot<TState>`. Keep `Id`
and `Timestamp` first on events if you follow the Eventhesis field-order
convention.

```csharp
using Microsoft.Extensions.DependencyInjection;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing;

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

Build the host, run the commands, and read the state back:

```csharp
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

`GetOrCreateAsync` loads existing events (or starts a new stream).
`HandleCommandAsync` authorizes if configured, dispatches `Handle(TCommand)`
(or `HandleAsync` when that is still overridden), and
appends the pending events. Guid overloads build `{type}:{id}` (with a tenant
prefix when one is present). Use `GetOrCreateAsync<T>(streamId)` when the
stream is not that shape.

## 4. Run Academy

```bash
dotnet run --project demos/LawnDart.Demo.Academy
```

No Docker. Optional SQL Server is a separate launch profile
(`--launch-profile SqlServer`). WebApi:

```bash
dotnet run --project demos/LawnDart.Demo.Academy.WebApi
```

## Next

- [Learning path](learning-path/README.md)
- [Eventhesis and LawnDart](EVENTHESIS.md)
