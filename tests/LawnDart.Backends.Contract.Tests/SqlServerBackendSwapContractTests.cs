using LawnDart.Backends.Contract.Tests.SwapApp;

namespace LawnDart.Backends.Contract.Tests;

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerBackendSwapContractTests(SqlServerContractFixture sql)
{
    [Fact]
    public async Task SameApplication_DispatchPersistReloadProjectReadBack()
    {
        await using var host = await SwapHost.StartSqlServerAsync(sql.ConnectionString);
        await BackendSwapContract.DispatchPersistReloadProjectReadBackAsync(host);
    }
}
