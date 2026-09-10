using System.Reflection;
using LawnDart.EventStore;
using LawnDart.Projections.Lightweight.Registration;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class ProjectionScannerTests
{
    private static readonly Assembly TestAssembly =
        typeof(Helpers.CounterSummaryProjection).Assembly;

    [Fact]
    public void Scan_DiscoversSingleStreamProjection()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        var counter = results.Single(r => r.StorageKey == "CounterSummary:v1");

        Assert.Equal(ProjectionKind.SingleStream, counter.Kind);
        Assert.Equal("CounterSummary", counter.LogicalName);
        Assert.Equal(1, counter.Version);
        Assert.Equal("CounterSummary:v1", counter.StorageKey);
        Assert.Equal("Counter", counter.StreamType);
        Assert.Equal(TenantScope.TenantScoped, counter.TenantScope);
        Assert.Equal(typeof(Helpers.CounterView), counter.ViewType);
        Assert.Equal(typeof(Helpers.CounterSummaryProjection), counter.HandlerType);
        Assert.True(counter.IsLatest);
        Assert.Equal(LatestVersionResolutionMode.SingleVersionImplicit, counter.LatestResolutionMode);
    }

    [Fact]
    public void Scan_DiscoveredSingleStreamProjection_HasEndpointConfig()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        var counter = results.Single(r => r.StorageKey == "CounterSummary:v1");

        Assert.NotNull(counter.Endpoint);
        Assert.Equal("/api/views/counters/{counterId}", counter.Endpoint!.Route);
        Assert.Equal("Counter.View", counter.Endpoint.RequiredPermission);
    }

    [Fact]
    public void Scan_DiscoversGlobalProjection()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        var global = results.Single(r => r.StorageKey == "GlobalTagIndex:v1");

        Assert.Equal(ProjectionKind.Global, global.Kind);
        Assert.Equal(TenantScope.SystemGlobal, global.TenantScope);
        Assert.Null(global.StreamType);
        Assert.Equal(typeof(Helpers.GlobalIndexView), global.ViewType);
    }

    [Fact]
    public void Scan_DiscoversDcbProjection()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        var dcb = results.Single(r => r.StorageKey == "CounterDcbFeed:v1");

        Assert.Equal(ProjectionKind.Dcb, dcb.Kind);
        Assert.Contains(
            EventTypeNameResolver.GetName(typeof(Helpers.CounterIncremented)),
            dcb.DcbQueryTypes);
    }

    [Fact]
    public void Scan_ProjectionWithoutEndpointAttribute_HasNullEndpoint()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        var noEndpoint = results.Single(r => r.StorageKey == "NoEndpointProjection:v1");

        Assert.Null(noEndpoint.Endpoint);
    }

    [Fact]
    public void Scan_IgnoresNonProjectionTypes()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        Assert.DoesNotContain(results, r => r.HandlerType == typeof(Helpers.NotAProjection));
    }

    [Fact]
    public void TryBuildRegistration_ReturnsNull_ForUndecorated_Class()
    {
        var result = ProjectionScanner.TryBuildRegistration(typeof(Helpers.NotAProjection));

        Assert.Null(result);
    }

    [Fact]
    public void ExtractViewType_ReturnsCorrectGenericArgument()
    {
        var viewType = ProjectionScanner.ExtractViewType(typeof(Helpers.CounterSummaryProjection));

        Assert.Equal(typeof(Helpers.CounterView), viewType);
    }

    [Fact]
    public void Scan_MultipleAssemblies_AggregatesResults()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ProjectionScanner.Scan(TestAssembly, TestAssembly));

        Assert.Contains("duplicate version registrations", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Scan_DiscoversMultiStreamProjection()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        var msp = results.Single(r => r.StorageKey == "OrderFulfillmentSummary:v1");

        Assert.Equal(ProjectionKind.MultiStream, msp.Kind);
        Assert.Equal(TenantScope.TenantScoped, msp.TenantScope);
        Assert.Null(msp.StreamType);
        Assert.Contains("test-projection-fixtures.order-created-local", msp.DcbQueryTypes);
        Assert.Contains("test-projection-fixtures.shipment-dispatched-local", msp.DcbQueryTypes);
        Assert.Equal(typeof(Helpers.FulfillmentView), msp.ViewType);
        Assert.Equal(typeof(Helpers.OrderFulfillmentSummaryProjection), msp.HandlerType);
    }

    [Fact]
    public void Scan_MultiStreamProjection_HasEndpointConfig()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        var msp = results.Single(r => r.StorageKey == "OrderFulfillmentSummary:v1");

        Assert.NotNull(msp.Endpoint);
        Assert.Equal("/api/views/fulfillment/{orderId}", msp.Endpoint!.Route);
    }

    [Fact]
    public void TryBuildRegistration_MultiStreamWithoutResolver_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ProjectionScanner.TryBuildRegistration(typeof(Helpers.BrokenMultiStreamProjection)));

        Assert.Contains("IMultiStreamEntityResolver", ex.Message);
    }

    [Fact]
    public void Scan_ExplicitLatestVersion_ResolvesLatestAlias()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        var v1 = results.Single(r => r.StorageKey == "VersionedCounterSummary:v1");
        var v2 = results.Single(r => r.StorageKey == "VersionedCounterSummary:v2");

        Assert.False(v1.IsLatest);
        Assert.True(v2.IsLatest);
        Assert.False(v1.DeclaredAsLatest);
        Assert.True(v2.DeclaredAsLatest);
        Assert.Equal(LatestVersionResolutionMode.Explicit, v1.LatestResolutionMode);
        Assert.Equal(LatestVersionResolutionMode.Explicit, v2.LatestResolutionMode);
    }

    [Fact]
    public void Scan_MissingExplicitLatest_FallsBackToHighestVersion()
    {
        var results = ProjectionScanner.Scan(TestAssembly);

        var v1 = results.Single(r => r.StorageKey == "FallbackCounterSummary:v1");
        var v2 = results.Single(r => r.StorageKey == "FallbackCounterSummary:v2");

        Assert.False(v1.IsLatest);
        Assert.True(v2.IsLatest);
        Assert.Equal(LatestVersionResolutionMode.HighestVersionFallback, v1.LatestResolutionMode);
        Assert.Equal(LatestVersionResolutionMode.HighestVersionFallback, v2.LatestResolutionMode);
    }

    [Fact]
    public void Scan_MultipleLatestVersions_Throws()
    {
        var finalize = typeof(ProjectionScanner)
            .GetMethod("FinalizeRegistrations", BindingFlags.NonPublic | BindingFlags.Static);

        Assert.NotNull(finalize);

        var registrations = new List<ProjectionRegistration>
        {
            new(
                handlerType: typeof(Helpers.VersionedCounterSummaryV1Projection),
                viewType: typeof(Helpers.CounterView),
                projectionName: "DuplicateLatestProjection",
                version: 1,
                kind: ProjectionKind.SingleStream,
                tenantScope: TenantScope.TenantScoped,
                streamType: "Counter",
                declaredAsLatest: true),
            new(
                handlerType: typeof(Helpers.VersionedCounterSummaryV2Projection),
                viewType: typeof(Helpers.CounterView),
                projectionName: "DuplicateLatestProjection",
                version: 2,
                kind: ProjectionKind.SingleStream,
                tenantScope: TenantScope.TenantScoped,
                streamType: "Counter",
                declaredAsLatest: true)
        };

        var ex = Assert.Throws<TargetInvocationException>(() =>
            finalize!.Invoke(null, [registrations]));

        Assert.Contains("multiple versions marked latest", ex.InnerException?.Message ?? ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
