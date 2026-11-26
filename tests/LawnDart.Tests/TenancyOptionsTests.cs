using LawnDart;
using Xunit;

namespace LawnDart.Tests;

public class TenancyOptionsTests
{
    [Fact]
    public void Tenancy_DefinesExpectedMembers()
    {
        Assert.Equal([Tenancy.Single, Tenancy.Shared, Tenancy.Isolated], Enum.GetValues<Tenancy>());
    }

    [Fact]
    public void LawnDartOptions_DefaultsToSharedAndNullSingleTenantId()
    {
        var options = new LawnDartOptions();

        Assert.Equal(Tenancy.Shared, options.Tenancy);
        Assert.Null(options.SingleTenantId);
        Assert.True(options.ValidateNames);
    }

    [Fact]
    public void LawnDartOptions_ObsoleteRequireTenantId_MapsToAndFromTenancy()
    {
        var options = new LawnDartOptions();

#pragma warning disable CS0618
        Assert.True(options.RequireTenantId);

        options.RequireTenantId = false;
        Assert.Equal(Tenancy.Single, options.Tenancy);
        Assert.False(options.RequireTenantId);

        options.RequireTenantId = true;
        Assert.Equal(Tenancy.Shared, options.Tenancy);
        Assert.True(options.RequireTenantId);
#pragma warning restore CS0618
    }

    [Fact]
    public void EnsureValid_ThrowsNotSupported_WhenIsolated()
    {
        var options = new LawnDartOptions { Tenancy = Tenancy.Isolated };

        var ex = Assert.Throws<NotSupportedException>(() => options.EnsureValid());
        Assert.Contains("Isolated", ex.Message, StringComparison.Ordinal);
        Assert.Contains("not supported", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void EnsureValid_ThrowsArgumentException_WhenSharedWithSingleTenantId()
    {
        var options = new LawnDartOptions
        {
            Tenancy = Tenancy.Shared,
            SingleTenantId = "acme"
        };

        var ex = Assert.Throws<ArgumentException>(() => options.EnsureValid());
        Assert.Equal("singleTenantId", ex.ParamName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(" acme")]
    [InlineData("acme ")]
    [InlineData("acme:sub")]
    [InlineData("acme!")]
    [InlineData("default")]
    public void EnsureValid_RejectsInvalidSingleTenantId_WhenValidateNamesIsTrue(string tenantId)
    {
        var options = new LawnDartOptions
        {
            Tenancy = Tenancy.Single,
            SingleTenantId = tenantId,
            ValidateNames = true
        };

        Assert.Throws<ArgumentException>(() => options.EnsureValid());
    }

    [Fact]
    public void EnsureValid_AllowsNonPatternSingleTenantId_WhenValidateNamesIsFalse_StillRejectsColonAndBlank()
    {
        var ok = new LawnDartOptions
        {
            Tenancy = Tenancy.Single,
            SingleTenantId = "acme!",
            ValidateNames = false
        };
        ok.EnsureValid();

        Assert.Throws<ArgumentException>(() => new LawnDartOptions
        {
            Tenancy = Tenancy.Single,
            SingleTenantId = "Acme:Corp",
            ValidateNames = false
        }.EnsureValid());

        Assert.Throws<ArgumentException>(() => new LawnDartOptions
        {
            Tenancy = Tenancy.Single,
            SingleTenantId = " ",
            ValidateNames = false
        }.EnsureValid());
    }
}
