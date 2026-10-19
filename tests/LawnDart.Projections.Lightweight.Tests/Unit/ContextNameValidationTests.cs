using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using LawnDart;
using LawnDart.Projections.Lightweight;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class ContextNameValidationTests
{
    [Fact]
    public void AddInMemoryProjectionStores_RejectsInvalidContextName()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(() => services.AddInMemoryProjectionStores("bad name"));
        Assert.Contains(LawnDartNames.Pattern, ex.Message);
    }

    [Fact]
    public void AddSqlProjectionStores_RejectsInvalidContextName()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(() =>
            services.AddSqlProjectionStores("bad name", "Server=unused"));
        Assert.Contains(LawnDartNames.Pattern, ex.Message);
    }

    [Fact]
    public void AddProjectionDebugServices_RejectsInvalidContextName()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(() => services.AddProjectionDebugServices(".bad"));
        Assert.Contains(LawnDartNames.Pattern, ex.Message);
    }

    [Fact]
    public void AddSqlProjectionStores_ValidateNamesFalse_AllowsExistingName()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.ValidateNames = false);
        services.AddSqlProjectionStores("My Context", "Server=unused");
        services.AddInMemoryProjectionStores("My Context");
        services.AddProjectionDebugServices("My Context");
    }

    [Fact]
    public async Task MapProjectionQueries_RejectsInvalidContextName()
    {
        using var host = new HostBuilder()
            .ConfigureWebHost(webHost =>
            {
                webHost.UseTestServer();
                webHost.ConfigureServices(services =>
                {
                    services.AddRouting();
                    services.AddLawnDart();
                });
                webHost.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints =>
                    {
                        var ex = Assert.Throws<ArgumentException>(() =>
                            endpoints.MapProjectionQueries("bad name"));
                        Assert.Contains(LawnDartNames.Pattern, ex.Message);
                    });
                });
            })
            .Build();

        await host.StartAsync();
    }
}
