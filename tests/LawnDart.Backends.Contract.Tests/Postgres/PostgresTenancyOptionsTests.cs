using LawnDart;
using LawnDart.EventSourcing.Postgres;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventStore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace LawnDart.Backends.Contract.Tests.Postgres;

public class PostgresTenancyOptionsTests
{
    private const string DummyConnectionString = "Host=localhost;Database=lawndart;Username=postgres;Password=postgres";

    [Fact]
    public void Options_DefaultsAndObsoleteRequireTenantId_MapCorrectly()
    {
        var opts = new PostgresEventStoreOptions();
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
            new PostgresEventStore(
                DummyConnectionString,
                options: new PostgresEventStoreOptions { Tenancy = Tenancy.Isolated }));
        Assert.Contains("Isolated", isolated.Message, StringComparison.Ordinal);

        Assert.Throws<ArgumentException>(() =>
            new PostgresEventStore(
                DummyConnectionString,
                options: new PostgresEventStoreOptions
                {
                    Tenancy = Tenancy.Shared,
                    SingleTenantId = "acme"
                }));
    }

    [Fact]
    public void UsePostgres_CopiesTenancyFromLawnDartOptions_AndRefusesIsolated()
    {
        var isolatedServices = new ServiceCollection();
        isolatedServices.AddLawnDart(o => o.Tenancy = Tenancy.Isolated);
        var isolatedEx = Assert.Throws<NotSupportedException>(() =>
            isolatedServices.AddBoundedContext("default").UsePostgres(o => o.ConnectionString = DummyConnectionString));
        Assert.Contains("Isolated", isolatedEx.Message, StringComparison.Ordinal);

        var services = new ServiceCollection();
        services.AddLawnDart(o =>
        {
            o.Tenancy = Tenancy.Single;
            o.SingleTenantId = "Acme_One";
            o.ValidateNames = false;
        });
        services.AddBoundedContext("ordering")
            .UsePostgres(o => o.ConnectionString = DummyConnectionString)
            .WithEventTypes<SampleEvent>();

        using var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredKeyedService<PostgresEventStoreOptions>("ordering");
        Assert.Equal(Tenancy.Single, options.Tenancy);
        Assert.Equal("Acme_One", options.SingleTenantId);
        Assert.False(options.ValidateNames);
        Assert.NotNull(sp.GetRequiredKeyedService<IEventStore>("ordering"));
    }

    private sealed class SampleEvent : IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    }
}
