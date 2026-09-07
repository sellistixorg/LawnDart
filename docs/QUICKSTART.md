# Quickstart (InMemory)

Zero infrastructure. .NET 10 SDK only.

## 1. Add project references

Packages are not on nuget.org yet. Clone this repository and
reference the projects you need:

```xml
<ItemGroup>
  <ProjectReference Include="path/to/LawnDart/src/LawnDart/LawnDart.csproj" />
  <ProjectReference Include="path/to/LawnDart/src/LawnDart.EventSourcing/LawnDart.EventSourcing.csproj" />
</ItemGroup>
```

A console host also needs `Microsoft.Extensions.DependencyInjection`.
Or pack to a local feed (see the root [README](../README.md)).

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
and `Timestamp` first on events — that is the Eventhesis contract.

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
`HandleCommandAsync` authorizes if configured, calls `HandleAsync`, and
appends the pending events.

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
- [Eventhesis compile contract](EVENTHESIS.md)
