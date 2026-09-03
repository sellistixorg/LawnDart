using LawnDart.Metadata;

namespace LawnDart.Tests.Metadata;

/// <summary>
/// Tests for CompositeTenantContextProvider (1.T6, 1.T8).
/// </summary>
public class CompositeTenantContextProviderTests
{
    private static ITenantContextProvider Returning(string? value)
        => new StubProvider(value);

    // ─────────── 1.T6: basic composition ───────────

    [Fact]
    public void GetTenantId_FirstProviderReturnsValue_UsesFirstProvider()
    {
        var composite = new CompositeTenantContextProvider(
        [
            Returning("first"),
            Returning("second")
        ]);
        Assert.Equal("first", composite.GetTenantId());
    }

    [Fact]
    public void GetTenantId_FirstProviderReturnsNull_FallsBackToSecond()
    {
        var composite = new CompositeTenantContextProvider(
        [
            Returning(null),
            Returning("second")
        ]);
        Assert.Equal("second", composite.GetTenantId());
    }

    [Fact]
    public void GetTenantId_AllProvidersReturnNull_ReturnsNull()
    {
        var composite = new CompositeTenantContextProvider(
        [
            Returning(null),
            Returning(null)
        ]);
        Assert.Null(composite.GetTenantId());
    }

    [Fact]
    public void GetTenantIdRequired_AllReturnNull_ThrowsWithProviderNames()
    {
        var composite = new CompositeTenantContextProvider(
        [
            Returning(null),
            Returning(null)
        ]);
        var ex = Assert.Throws<InvalidOperationException>(() => composite.GetTenantIdRequired());
        Assert.Contains("StubProvider", ex.Message);
    }

    [Fact]
    public void GetTenantIdRequired_FirstReturnsValue_ReturnsWithoutThrowing()
    {
        var composite = new CompositeTenantContextProvider([Returning("shop")]);
        Assert.Equal("shop", composite.GetTenantIdRequired());
    }

    [Fact]
    public void Constructor_EmptyList_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(
            () => new CompositeTenantContextProvider(Array.Empty<ITenantContextProvider>()));
        Assert.Contains("At least one", ex.Message);
    }

    [Fact]
    public void Constructor_NullList_Throws()
        => Assert.Throws<ArgumentNullException>(
            () => new CompositeTenantContextProvider(null!));

    // ─────────── 1.T8: composite with FixedTenantContextProvider as last element ───────────

    [Fact]
    public void GetTenantId_WithFixedFallback_AlwaysResolvesEvenWhenOthersReturnNull()
    {
        var composite = new CompositeTenantContextProvider(
        [
            Returning(null),
            Returning(null),
            new FixedTenantContextProvider("shop")
        ]);
        Assert.Equal("shop", composite.GetTenantId());
    }

    [Fact]
    public void GetTenantIdRequired_WithFixedFallback_NeverThrows()
    {
        var composite = new CompositeTenantContextProvider(
        [
            Returning(null),
            new FixedTenantContextProvider("shop")
        ]);
        var result = composite.GetTenantIdRequired(); // must not throw
        Assert.Equal("shop", result);
    }

    [Fact]
    public void GetTenantId_WithAmbientAndFixedFallback_AmbientWinsWhenSet()
    {
        var ambient = new AmbientTenantContextProvider();
        var composite = new CompositeTenantContextProvider(
        [
            ambient,
            new FixedTenantContextProvider("shop")
        ]);

        // Without ambient context the fixed fallback wins
        Assert.Equal("shop", composite.GetTenantId());

        // With ambient context the ambient value wins
        using (ambient.SetTenant("acme"))
        {
            Assert.Equal("acme", composite.GetTenantId());
        }

        // After scope the fixed fallback wins again
        Assert.Equal("shop", composite.GetTenantId());
    }

    private sealed class StubProvider(string? value) : ITenantContextProvider
    {
        public string? GetTenantId() => value;
        public string GetTenantIdRequired() => value ?? throw new InvalidOperationException();
    }
}
