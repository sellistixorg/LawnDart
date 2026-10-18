using LawnDart.Metadata;
using LawnDart;

namespace LawnDart.Tests.Metadata;

/// <summary>
/// Tests for FixedTenantContextProvider (1.T7).
/// </summary>
public class FixedTenantContextProviderTests
{
    [Fact]
    public void GetTenantId_AlwaysReturnsConfiguredValue()
    {
        var provider = new FixedTenantContextProvider("shop");
        Assert.Equal("shop", provider.GetTenantId());
    }

    [Fact]
    public void GetTenantIdRequired_AlwaysReturnsConfiguredValue_NeverThrows()
    {
        var provider = new FixedTenantContextProvider("shop");
        var result = provider.GetTenantIdRequired(); // must not throw
        Assert.Equal("shop", result);
    }

    [Fact]
    public void GetTenantId_MultipleCallsReturnSameValue()
    {
        var provider = new FixedTenantContextProvider("platform");
        Assert.Equal(provider.GetTenantId(), provider.GetTenantId());
    }

    [Fact]
    public void Constructor_NullTenantId_Throws()
        => Assert.Throws<ArgumentNullException>(() => new FixedTenantContextProvider(null!));

    [Fact]
    public void Constructor_WhitespaceTenantId_Throws()
        => Assert.Throws<ArgumentException>(() => new FixedTenantContextProvider("   "));

    [Fact]
    public void Constructor_InvalidTenantId_NamesTheRule()
    {
        var ex = Assert.Throws<ArgumentException>(() => new FixedTenantContextProvider("bad name"));
        Assert.Contains(LawnDartNames.Pattern, ex.Message);
    }

    [Fact]
    public void Constructor_ValidateNamesFalse_AllowsExistingId_RejectsColon()
    {
        var provider = new FixedTenantContextProvider("My Tenant", validateNames: false);
        Assert.Equal("My Tenant", provider.GetTenantId());

        var ex = Assert.Throws<ArgumentException>(() =>
            new FixedTenantContextProvider("a:b", validateNames: false));
        Assert.Contains(":", ex.Message);
    }

    [Fact]
    public void TwoInstances_SameValue_BehaveIndependently()
    {
        var a = new FixedTenantContextProvider("shop");
        var b = new FixedTenantContextProvider("shop");
        Assert.Equal(a.GetTenantId(), b.GetTenantId());
        Assert.Equal(a.GetTenantIdRequired(), b.GetTenantIdRequired());
    }
}
