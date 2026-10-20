namespace LawnDart.Backends.Contract.Tests;

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerEventLogContractTests : EventLogContractTests
{
    public SqlServerEventLogContractTests(SqlServerContractFixture fixture)
        : base(fixture.Backend)
    {
    }
}
