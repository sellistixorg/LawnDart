using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using LawnDart.EventStore;
using LawnDart.EventSourcing.SqlServer;

namespace LawnDart.EventSourcing.SqlServer.Tests.Tagging;

public class TagFormatOptionsRegistrationTests
{
    [Fact]
    public void UseSqlServer_CopiesNonDefaultTagFlagsOntoEventSourcingOptions()
    {
        var services = new ServiceCollection();
        services.AddLawnDart();
        services.AddBoundedContext("shop").UseSqlServer(o =>
        {
            o.ConnectionString = "Server=localhost;Database=lawndart;User ID=sa;Password=x;TrustServerCertificate=true";
            o.WarnOnUnknownTag = false;
            o.FormatAggregateFallbackTag = true;
        });

        var options = services.BuildServiceProvider().GetRequiredService<IOptions<EventSourcingOptions>>().Value;

        Assert.False(options.WarnOnUnknownTag);
        Assert.True(options.FormatAggregateFallbackTag);
    }

    [Fact]
    public void UseSqlServer_LeavesAPriorConfigureWhenTheCallbackKeepsDefaults()
    {
        var services = new ServiceCollection();
        services.Configure<EventSourcingOptions>(o => o.WarnOnUnknownTag = false);
        services.AddLawnDart();
        services.AddBoundedContext("shop").UseSqlServer(o =>
        {
            o.ConnectionString = "Server=localhost;Database=lawndart;User ID=sa;Password=x;TrustServerCertificate=true";
        });

        var options = services.BuildServiceProvider().GetRequiredService<IOptions<EventSourcingOptions>>().Value;

        Assert.False(options.WarnOnUnknownTag);
        Assert.False(options.FormatAggregateFallbackTag);
    }
}
