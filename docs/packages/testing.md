# LawnDart.Testing

Given / when / then specs and messaging harnesses. `BddTestContext.CreateInMemory`
is the only factory — no Docker.

Take it for domain tests. SQL Server integration tests live in the
`*.SqlServer.Tests` projects and are tagged `Category=Integration`.

## Registration

No host registration. Reference the package from the test project:

```csharp
await using var ctx = BddTestContext.CreateInMemory();

await AggregateSpec
    .For<Counter>(ctx, id)
    .Given(new CounterCreated(Guid.NewGuid(), DateTime.UtcNow, id))
    .When(new IncrementCommand(Guid.NewGuid(), id))
    .ThenEmittedEvent<CounterIncremented>(e => e.CounterId == id)
    .RunAsync();
```

Also ships `DcbSpec`, `ReactorTestHarness<TReactor, TEvent>`, and
`WaitForAsync` for eventual-consistency assertions.

## Related

- [Package map](README.md)
- [BDD testing](../testing/BDD_TESTING.md)
- [Testing your aggregate](../learning-path/03-testing-your-aggregate.md)
- [Testing EDA](../learning-path/07-testing-eda.md)
