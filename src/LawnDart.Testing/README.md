# LawnDart.Testing

Given / when / then specs and messaging harnesses. Start with
`BddTestContext.CreateInMemory`.

```csharp
await using var ctx = BddTestContext.CreateInMemory();
await AggregateSpec.For<Order>(ctx, orderId)
    .Given(...)
    .When(...)
    .ThenEmittedTypes(...)
    .RunAsync();
```
