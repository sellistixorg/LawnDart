using LawnDart.Backends.Contract.Tests.SwapApp;

namespace LawnDart.Backends.Contract.Tests;

[Trait("Category", "Contract")]
public sealed class InMemoryBackendSwapContractTests
{
    [Fact]
    public async Task SameApplication_DispatchPersistReloadProjectReadBack()
    {
        await using var host = await SwapHost.StartInMemoryAsync();
        await BackendSwapContract.DispatchPersistReloadProjectReadBackAsync(host);
    }
}
