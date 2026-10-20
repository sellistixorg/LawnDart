namespace LawnDart.Backends.Contract.Tests;

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresEventLogContractTests : EventLogContractTests
{
    public PostgresEventLogContractTests(PostgresContractFixture fixture)
        : base(fixture.Backend)
    {
    }
}
