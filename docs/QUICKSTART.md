# Quickstart (InMemory)

Zero infrastructure. .NET 10 SDK only.

## 1. Add project references

Packages are not on nuget.org yet (`0.1.0-alpha`). Clone this repository and
reference the projects you need:

```xml
<ItemGroup>
  <ProjectReference Include="path/to/LawnDart/src/LawnDart/LawnDart.csproj" />
  <ProjectReference Include="path/to/LawnDart/src/LawnDart.EventSourcing/LawnDart.EventSourcing.csproj" />
</ItemGroup>
```

Or pack to a local feed (see the root [README](../README.md)).

## 2. Register the host

```csharp
services.AddLawnDart(o => o.RequireTenantId = false);
var ctx = services.AddBoundedContext("default");
ctx.UseInMemory();
```

## 3. Handle a command

Define an `ICommand`, an `IEvent`, and an `AggregateRoot<TState>` (or DCB
entity). The event store appends events when the command succeeds.

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

- [Eventhesis compile contract](EVENTHESIS.md)
- [Learning path](learning-path/README.md)
