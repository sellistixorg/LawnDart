using LawnDart.Backends.Contract.Tests.SwapApp;

namespace LawnDart.Backends.Contract.Tests;

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresBackendSwapContractTests(PostgresContractFixture postgres)
{
    [Fact]
    public async Task SameApplication_DispatchPersistReloadProjectReadBack()
    {
        var schema = "s" + Guid.NewGuid().ToString("N");
        await using var host = await SwapHost.StartPostgresAsync(postgres.AdminConnectionString, schema);
        await BackendSwapContract.DispatchPersistReloadProjectReadBackAsync(host);
    }
}
