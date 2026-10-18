using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Tagging;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

public class TagFormatRegistrationTests
{
    [Fact]
    public void WithTagFormat_Type_RegistersKeyedSingleton()
    {
        var services = BaseServices();
        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithEventTypes(typeof(CatalogPlaceholderEvent))
            .WithTagFormat<AlphaFormat>();

        var format = services.BuildServiceProvider().GetKeyedService<ITagFormat>("alpha");

        Assert.IsType<AlphaFormat>(format);
    }

    [Fact]
    public void WithTagFormat_InstanceAndFactory_RegisterKeyedSingletons()
    {
        var services = BaseServices();
        var instance = new AlphaFormat();
        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithEventTypes(typeof(CatalogPlaceholderEvent))
            .WithTagFormat(instance);
        services.AddBoundedContext("beta")
            .UseInMemory()
            .WithEventTypes(typeof(CatalogPlaceholderEvent))
            .WithTagFormat(_ => new BetaFormat());

        var sp = services.BuildServiceProvider();

        Assert.Same(instance, sp.GetRequiredKeyedService<ITagFormat>("alpha"));
        Assert.IsType<BetaFormat>(sp.GetRequiredKeyedService<ITagFormat>("beta"));
    }

    [Fact]
    public async Task Repositories_PreferKeyedFormat_AndFallBackToGlobal()
    {
        var services = BaseServices();
        services.Configure<EventSourcingOptions>(o => o.FormatAggregateFallbackTag = true);
        services.AddLawnDart(o => o.RequireTenantId = true);
        services.AddTagFormat<GlobalFormat>();
        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithEventTypes(typeof(CatalogPlaceholderEvent))
            .WithTagFormat<AlphaFormat>();
        services.AddBoundedContext("beta")
            .UseInMemory()
            .WithEventTypes(typeof(CatalogPlaceholderEvent));

        var sp = services.BuildServiceProvider();
        var alphaId = Guid.NewGuid();
        var betaId = Guid.NewGuid();

        await SaveAsync(sp.GetRequiredKeyedService<IAggregateRepository>("alpha"), alphaId);
        await SaveAsync(sp.GetRequiredKeyedService<IAggregateRepository>("beta"), betaId);

        var alphaStore = sp.GetRequiredKeyedService<IEventStore>("alpha");
        var betaStore = sp.GetRequiredKeyedService<IEventStore>("beta");
        var alphaTags = (await alphaStore.ReadStreamAsync($"acct:ProbeAggregate:{alphaId}"))[0].Tags;
        var betaTags = (await betaStore.ReadStreamAsync($"acct:ProbeAggregate:{betaId}"))[0].Tags;

        Assert.Contains($"alpha:probeaggregate:{alphaId:D}", alphaTags);
        Assert.Contains($"global:probeaggregate:{betaId:D}", betaTags);
        Assert.DoesNotContain($"ProbeAggregate:{alphaId:D}", alphaTags);
    }

    [Fact]
    public void UseInMemory_Callback_SetsEventSourcingOptions()
    {
        var services = BaseServices();
        services.AddBoundedContext("default")
            .UseInMemory(o =>
            {
                o.WarnOnUnknownTag = false;
                o.FormatAggregateFallbackTag = true;
            })
            .WithEventTypes(typeof(CatalogPlaceholderEvent));

        var options = services.BuildServiceProvider().GetRequiredService<IOptions<EventSourcingOptions>>().Value;

        Assert.False(options.WarnOnUnknownTag);
        Assert.True(options.FormatAggregateFallbackTag);
    }

    [Fact]
    public async Task TwoContexts_WithDifferentFormats_WarnIndependently()
    {
        var logger = new ListLogger<DcbRepository>();
        var services = BaseServices();
        services.AddSingleton<ILogger<DcbRepository>>(logger);
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithEventTypes(typeof(CatalogPlaceholderEvent))
            .WithTagFormat<AlphaFormat>();
        services.AddBoundedContext("beta")
            .UseInMemory()
            .WithEventTypes(typeof(CatalogPlaceholderEvent))
            .WithTagFormat<BetaFormat>();

        var sp = services.BuildServiceProvider();
        var alpha = sp.GetRequiredKeyedService<IDcbRepository>("alpha");
        var beta = sp.GetRequiredKeyedService<IDcbRepository>("beta");
        var first = $"Student:{Guid.NewGuid():D}";
        var second = $"Student:{Guid.NewGuid():D}";

        await alpha.GetStateAsync<EmptyState>([first]);
        await alpha.GetStateAsync<EmptyState>([second]);
        await beta.GetStateAsync<EmptyState>([first]);
        await beta.GetStateAsync<EmptyState>([second]);

        Assert.Equal(2, logger.Warnings.Count(message => message.Contains(first, StringComparison.Ordinal)));
        Assert.Equal(0, logger.Warnings.Count(message => message.Contains(second, StringComparison.Ordinal)));
    }

    [Fact]
    public async Task Configure_WarnOnUnknownTagFalse_ReachesTheRepository()
    {
        var logger = new ListLogger<DcbRepository>();
        var services = BaseServices();
        services.AddSingleton<ILogger<DcbRepository>>(logger);
        services.Configure<EventSourcingOptions>(o => o.WarnOnUnknownTag = false);
        services.AddLawnDart(o => o.RequireTenantId = false);
        services.AddBoundedContext("default")
            .UseInMemory()
            .WithEventTypes(typeof(CatalogPlaceholderEvent));

        var repo = services.BuildServiceProvider().GetRequiredService<IDcbRepository>();
        var bad = $"Student:{Guid.NewGuid():D}";

        await repo.GetStateAsync<EmptyState>([bad]);

        Assert.Empty(logger.Warnings);
    }

    private static ServiceCollection BaseServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<Metadata.ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });
        return services;
    }

    private static async Task SaveAsync(IAggregateRepository repository, Guid id)
    {
        var aggregate = new ProbeAggregate();
        aggregate.SetStreamId($"acct:ProbeAggregate:{id}");
        aggregate.Raise(new CatalogPlaceholderEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(aggregate, new Metadata.CommandMetadata { TenantId = "acct" });
    }

    private sealed class AlphaFormat : ITagFormat
    {
        public string Tag(string type, Guid id) => Tag(type, id.ToString("D"));
        public string Tag(string type, string id) => $"alpha:{type.ToLowerInvariant()}:{id}";
    }

    private sealed class BetaFormat : ITagFormat
    {
        public string Tag(string type, Guid id) => Tag(type, id.ToString("D"));
        public string Tag(string type, string id) => $"beta:{type.ToLowerInvariant()}:{id}";
    }

    private sealed class GlobalFormat : ITagFormat
    {
        public string Tag(string type, Guid id) => Tag(type, id.ToString("D"));
        public string Tag(string type, string id) => $"global:{type.ToLowerInvariant()}:{id}";
    }

    private sealed class ProbeAggregate : AggregateRoot<ProbeState>
    {
        public void Raise(IEvent @event) => Apply(@event);

        protected override void ApplyEventToState(IEvent @event) { }
    }

    private sealed class ProbeState : IState;

    private sealed class EmptyState : IState;

    private sealed class ListLogger<T> : ILogger<T>
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }
}
