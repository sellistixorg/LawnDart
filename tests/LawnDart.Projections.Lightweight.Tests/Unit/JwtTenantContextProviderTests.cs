using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using NSubstitute;
using LawnDart.Projections.Lightweight.Security;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class JwtTenantContextProviderTests
{
    private static JwtTenantContextProvider BuildProvider(ClaimsPrincipal? user = null)
    {
        var httpContext = new DefaultHttpContext
        {
            User = user ?? new ClaimsPrincipal()
        };

        var accessor = Substitute.For<IHttpContextAccessor>();
        accessor.HttpContext.Returns(httpContext);

        return new JwtTenantContextProvider(accessor);
    }

    [Fact]
    public void GetTenantId_Returns_TenantId_Claim()
    {
        var user = new ClaimsPrincipal(new ClaimsIdentity([new Claim("tenant_id", "acme-corp")]));
        var provider = BuildProvider(user);

        Assert.Equal("acme-corp", provider.GetTenantId());
    }

    [Fact]
    public void GetTenantId_Falls_Back_To_Tid_Claim()
    {
        var user = new ClaimsPrincipal(new ClaimsIdentity([new Claim("tid", "azure-tid-value")]));
        var provider = BuildProvider(user);

        Assert.Equal("azure-tid-value", provider.GetTenantId());
    }

    [Fact]
    public void GetTenantId_Prefers_TenantId_Over_Tid()
    {
        var user = new ClaimsPrincipal(new ClaimsIdentity(
        [
            new Claim("tenant_id", "preferred"),
            new Claim("tid", "fallback")
        ]));
        var provider = BuildProvider(user);

        Assert.Equal("preferred", provider.GetTenantId());
    }

    [Fact]
    public void GetTenantId_ReturnsNull_When_NoClaims()
    {
        var provider = BuildProvider();

        Assert.Null(provider.GetTenantId());
    }

    [Fact]
    public void GetTenantId_ReturnsNull_When_HttpContext_Is_Null()
    {
        var accessor = Substitute.For<IHttpContextAccessor>();
        accessor.HttpContext.Returns((HttpContext?)null);
        var provider = new JwtTenantContextProvider(accessor);

        Assert.Null(provider.GetTenantId());
    }

    [Fact]
    public void GetTenantIdRequired_Returns_Value_When_Present()
    {
        var user = new ClaimsPrincipal(new ClaimsIdentity([new Claim("tenant_id", "my-tenant")]));
        var provider = BuildProvider(user);

        Assert.Equal("my-tenant", provider.GetTenantIdRequired());
    }

    [Fact]
    public void GetTenantIdRequired_Throws_When_Claim_Missing()
    {
        var provider = BuildProvider();

        Assert.Throws<InvalidOperationException>(() => provider.GetTenantIdRequired());
    }

    [Fact]
    public void Constructor_Throws_When_Accessor_Is_Null()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new JwtTenantContextProvider(null!));
    }
}
