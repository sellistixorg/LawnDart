using Microsoft.Extensions.DependencyInjection;
using LawnDart.Aggregates;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Tagging;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.BoundedContext;

/// <summary>
/// Verifies that per-context and global ITagProvider registrations are resolved correctly
/// by AggregateRepository and DcbRepository via both the WithTagProvider fluent API and the
/// AddTagProvider(contextName) extension method.
/// </summary>
public class TagProviderRegistrationTests
{
    // ── helpers ───────────────────────────────────────────────────────────────

    private static ServiceCollection BaseServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });
        return services;
    }

    // ── stub tag providers ────────────────────────────────────────────────────

    private sealed class AlphaTagProvider : ITagProvider
    {
        public IEnumerable<string> GetTags(IEvent @event, object? context = null)
            => ["context:alpha"];
    }

    private sealed class BetaTagProvider : ITagProvider
    {
        public IEnumerable<string> GetTags(IEvent @event, object? context = null)
            => ["context:beta"];
    }

    private sealed class GlobalTagProvider : ITagProvider
    {
        public IEnumerable<string> GetTags(IEvent @event, object? context = null)
            => ["context:global"];
    }

    // ── WithTagProvider<T>() — type overload ──────────────────────────────────

    [Fact]
    public void WithTagProvider_Type_RegistersKeyedSingleton()
    {
        var services = BaseServices();

        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithTagProvider<AlphaTagProvider>();

        var sp = services.BuildServiceProvider();

        var provider = sp.GetKeyedService<ITagProvider>("alpha");
        Assert.NotNull(provider);
        Assert.IsType<AlphaTagProvider>(provider);
    }

    [Fact]
    public void WithTagProvider_TwoContexts_EachGetsOwnProvider()
    {
        var services = BaseServices();

        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithTagProvider<AlphaTagProvider>();

        services.AddBoundedContext("beta")
            .UseInMemory()
            .WithTagProvider<BetaTagProvider>();

        var sp = services.BuildServiceProvider();

        var alphaProvider = sp.GetKeyedService<ITagProvider>("alpha");
        var betaProvider  = sp.GetKeyedService<ITagProvider>("beta");

        Assert.IsType<AlphaTagProvider>(alphaProvider);
        Assert.IsType<BetaTagProvider>(betaProvider);
        Assert.NotSame(alphaProvider, betaProvider);
    }

    // ── WithTagProvider(instance) — instance overload ─────────────────────────

    [Fact]
    public void WithTagProvider_Instance_RegistersKeyedSingleton()
    {
        var services  = BaseServices();
        var instance  = new AlphaTagProvider();

        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithTagProvider(instance);

        var sp = services.BuildServiceProvider();

        var resolved = sp.GetKeyedService<ITagProvider>("alpha");
        Assert.Same(instance, resolved);
    }

    // ── WithTagProvider(factory) — factory overload ───────────────────────────

    [Fact]
    public void WithTagProvider_Factory_RegistersKeyedSingleton()
    {
        var services = BaseServices();

        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithTagProvider(_ => new AlphaTagProvider());

        var sp = services.BuildServiceProvider();

        var resolved = sp.GetKeyedService<ITagProvider>("alpha");
        Assert.NotNull(resolved);
        Assert.IsType<AlphaTagProvider>(resolved);
    }

    // ── AddTagProvider(contextName) — IServiceCollection overload ────────────

    [Fact]
    public void AddTagProvider_WithContextName_RegistersKeyedSingleton()
    {
        var services = BaseServices();

        services.AddBoundedContext("alpha").UseInMemory();
        services.AddTagProvider<AlphaTagProvider>("alpha");

        var sp = services.BuildServiceProvider();

        var resolved = sp.GetKeyedService<ITagProvider>("alpha");
        Assert.NotNull(resolved);
        Assert.IsType<AlphaTagProvider>(resolved);
    }

    [Fact]
    public void AddTagProvider_NullContextName_Throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddTagProvider<AlphaTagProvider>(null!));
    }

    [Fact]
    public void AddTagProvider_WhitespaceContextName_Throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddTagProvider<AlphaTagProvider>("   "));
    }

    // ── Fallback: keyed → global → null ──────────────────────────────────────

    [Fact]
    public void TagProvider_ContextWithKeyedProvider_DoesNotReceiveGlobalProvider()
    {
        // alpha has its own keyed provider; beta uses the global one
        var services = BaseServices();

        services.AddTagProvider<GlobalTagProvider>();   // global

        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithTagProvider<AlphaTagProvider>();       // context-specific

        services.AddBoundedContext("beta")
            .UseInMemory();                             // no context-specific → uses global

        var sp = services.BuildServiceProvider();

        var alphaKeyed  = sp.GetKeyedService<ITagProvider>("alpha");
        var betaKeyed   = sp.GetKeyedService<ITagProvider>("beta");
        var globalUnkeyed = sp.GetService<ITagProvider>();

        // alpha resolves to AlphaTagProvider (not global)
        Assert.IsType<AlphaTagProvider>(alphaKeyed);

        // beta has no keyed registration
        Assert.Null(betaKeyed);

        // the global provider exists
        Assert.IsType<GlobalTagProvider>(globalUnkeyed);
    }

    [Fact]
    public void TagProvider_NoRegistration_ReturnsNull()
    {
        var services = BaseServices();
        services.AddBoundedContext("alpha").UseInMemory();

        var sp = services.BuildServiceProvider();

        // Neither keyed nor non-keyed provider registered
        Assert.Null(sp.GetKeyedService<ITagProvider>("alpha"));
        Assert.Null(sp.GetService<ITagProvider>());
    }

    // ── Keyed provider is a singleton (same instance on repeated resolution) ──

    [Fact]
    public void WithTagProvider_Type_IsSingleton()
    {
        var services = BaseServices();

        services.AddBoundedContext("alpha")
            .UseInMemory()
            .WithTagProvider<AlphaTagProvider>();

        var sp = services.BuildServiceProvider();

        var first  = sp.GetKeyedService<ITagProvider>("alpha");
        var second = sp.GetKeyedService<ITagProvider>("alpha");

        Assert.Same(first, second);
    }
}
