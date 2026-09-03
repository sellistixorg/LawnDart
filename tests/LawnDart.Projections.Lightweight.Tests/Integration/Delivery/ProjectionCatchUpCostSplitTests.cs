using LawnDart.Projections.Lightweight.Hosting;
using Xunit.Abstractions;

namespace LawnDart.Projections.Lightweight.Tests.Integration.Delivery;

/// <summary>
/// Splits catch-up P into Subscribe drain, Handle/flush, read-cache JSON, and
/// shared-pipe fan-out vs per-runner typed Subscribe. Performance is recorded,
/// not asserted. Override N with <c>LAWNDART_CATCHUP_COST_N</c>.
/// </summary>
[Trait("Category", "Delivery")]
[Trait("Category", "Integration")]
public sealed class ProjectionCatchUpCostSplitTests
{
    private readonly ITestOutputHelper _output;

    public ProjectionCatchUpCostSplitTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task InMemory_layers_print_P_split()
    {
        var n = CatchUpCostHarness.ResolveTickCount();
        var timeout = n <= CatchUpCostHarness.CiTickCount
            ? TimeSpan.FromSeconds(30)
            : TimeSpan.FromMinutes(4);

        // JIT / first-runner warmup — discarded so CacheOff is not the cold layer.
        _ = await CatchUpCostHarness.MeasureRunnerAsync(
            "warmup",
            Math.Min(200, n),
            sharedSubscribe: true,
            ProjectionReadCacheMode.Hot,
            includeNoopShops: false,
            timeout);

        var rows = new List<CatchUpCostRow>
        {
            await CatchUpCostHarness.MeasureSubscribeDrainAsync(n, timeout),
            await CatchUpCostHarness.MeasureRunnerAsync(
                "PlaidOnly_CacheOff", n, sharedSubscribe: true,
                ProjectionReadCacheMode.Off, includeNoopShops: false, timeout),
            await CatchUpCostHarness.MeasureRunnerAsync(
                "PlaidOnly_CacheHot", n, sharedSubscribe: true,
                ProjectionReadCacheMode.Hot, includeNoopShops: false, timeout),
            await CatchUpCostHarness.MeasureRunnerAsync(
                "SharedPipe_PlaidPlus4Noop_CacheHot", n, sharedSubscribe: true,
                ProjectionReadCacheMode.Hot, includeNoopShops: true, timeout),
            await CatchUpCostHarness.MeasureRunnerAsync(
                "PerRunner_PlaidPlus4Noop_CacheHot", n, sharedSubscribe: false,
                ProjectionReadCacheMode.Hot, includeNoopShops: true, timeout)
        };

        CatchUpCostHarness.AssertCorrectness(rows, n);
        _output.WriteLine(CatchUpCostHarness.FormatTable(rows));
    }
}
