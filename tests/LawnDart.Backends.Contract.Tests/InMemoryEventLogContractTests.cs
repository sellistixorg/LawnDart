namespace LawnDart.Backends.Contract.Tests;

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryEventLogContractTests : EventLogContractTests
{
    public InMemoryEventLogContractTests(InMemoryContractFixture fixture)
        : base(fixture.Backend)
    {
    }
}
