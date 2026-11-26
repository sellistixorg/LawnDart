using LawnDart;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

public class SqlServerTenancyOptionsTests
{
    private const string DummyConnectionString = "Server=localhost;Database=LawnDartTest;Integrated Security=true;TrustServerCertificate=true;";

    [Fact]
    public void Options_DefaultsAndObsoleteRequireTenantId_MapCorrectly()
    {
        var opts = new SqlServerEventStoreOptions();
        Assert.Equal(Tenancy.Shared, opts.Tenancy);
        Assert.Null(opts.SingleTenantId);

#pragma warning disable CS0618
        Assert.True(opts.RequireTenantId);
        opts.RequireTenantId = false;
        Assert.Equal(Tenancy.Single, opts.Tenancy);
        Assert.False(opts.RequireTenantId);

        opts.RequireTenantId = true;
        Assert.Equal(Tenancy.Shared, opts.Tenancy);
        Assert.True(opts.RequireTenantId);
#pragma warning restore CS0618
    }

    [Fact]
    public void Constructor_RefusesIsolatedAndInvalidSingleTenantId()
    {
        var isolated = Assert.Throws<NotSupportedException>(() =>
            new SqlServerEventStore(
                DummyConnectionString,
                options: new SqlServerEventStoreOptions { Tenancy = Tenancy.Isolated }));
        Assert.Contains("Isolated", isolated.Message, StringComparison.Ordinal);

        Assert.Throws<ArgumentException>(() =>
            new SqlServerEventStore(
                DummyConnectionString,
                options: new SqlServerEventStoreOptions
                {
                    Tenancy = Tenancy.Shared,
                    SingleTenantId = "acme"
                }));
    }

    [Fact]
    public void UseSqlServer_CopiesTenancyFromLawnDartOptions_AndRefusesIsolated()
    {
        var isolatedServices = new ServiceCollection();
        isolatedServices.AddLawnDart(o => o.Tenancy = Tenancy.Isolated);
        var isolatedEx = Assert.Throws<NotSupportedException>(() =>
            isolatedServices.AddBoundedContext("default").UseSqlServer(o => o.ConnectionString = DummyConnectionString));
        Assert.Contains("Isolated", isolatedEx.Message, StringComparison.Ordinal);

        var services = new ServiceCollection();
        services.AddLawnDart(o =>
        {
            o.Tenancy = Tenancy.Single;
            o.SingleTenantId = "Acme_One";
            o.ValidateNames = false;
        });
        services.AddBoundedContext("ordering")
            .UseSqlServer(o => o.ConnectionString = DummyConnectionString)
            .WithEventTypes<SampleEvent>();

        using var sp = services.BuildServiceProvider();
        var store = Assert.IsType<SqlServerEventStore>(sp.GetRequiredKeyedService<IEventStore>("ordering"));
        Assert.NotNull(store);
    }

    private sealed class SampleEvent : IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    }
}
