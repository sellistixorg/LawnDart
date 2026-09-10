using System.Text.Json;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Sdk;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

/// <summary>
/// Unmatched types must not <c>NoteApplied</c>. Runner cursor may still advance.
/// </summary>
public sealed class CompiledHandleNoteAppliedTests
{
    [Fact]
    public async Task Global_UnmatchedThenMatched_DirtiesOnlyOnHandle_ViewLastAppliedIsMatchSequence()
    {
        var store = new InMemoryEventStore();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(10),
            CheckpointInterval = 1,
            BatchSize = 10,
            SkipTailFlushWhileCatchingUp = false
        };

        var unmatched = new OrderCreatedLocal(Guid.NewGuid(), DateTime.UtcNow, Guid.NewGuid(), "noise");
        await store.AppendAsync("system:Other:1", [unmatched], metadata: Meta());
        var unmatchedSeq = await store.GetCurrentSequenceAsync();

        var reg = ProjectionScanner.TryBuildRegistration(typeof(GlobalTagIndexProjection))!;
        var runner = new LightweightProjectionRunnerService(
            reg, store, views, checkpoints, new SingleNodePartitioningService(), options);
        await runner.StartAsync(CancellationToken.None);

        await WaitForRunnerCursorAsync(runner, unmatchedSeq, TimeSpan.FromSeconds(10));

        var viewAfterNoise = await views.GetViewWithCheckpointAsync("GlobalTagIndex:v1", "global");
        Assert.Null(viewAfterNoise);

        var matched = new GlobalTagged(Guid.NewGuid(), DateTime.UtcNow, "alpha");
        await store.AppendAsync("system:Tags:1", [matched], metadata: Meta());
        var matchedSeq = await store.GetCurrentSequenceAsync();

        var viewJson = await ProjectionRunnerTestHarness.WaitForViewAsync(
            views,
            "GlobalTagIndex:v1",
            "global",
            TimeSpan.FromSeconds(10),
            json => JsonSerializer.Deserialize<GlobalIndexView>(json, ProjectionViewJson.Read)?.TotalTags == 1);
        Assert.NotNull(viewJson);

        var saved = await views.GetViewWithCheckpointAsync("GlobalTagIndex:v1", "global");
        Assert.NotNull(saved);
        Assert.Equal(matchedSeq, saved.Value.Checkpoint);
        Assert.NotEqual(unmatchedSeq, saved.Value.Checkpoint);

        var runnerCheckpoint = await checkpoints.GetCheckpointAsync("GlobalTagIndex:v1", 0);
        Assert.NotNull(runnerCheckpoint);
        Assert.True(
            runnerCheckpoint!.LastSequencePosition >= matchedSeq,
            "Runner cursor/checkpoint advances through unmatched and matched events.");

        await runner.StopAsync(CancellationToken.None);
    }

    [Fact]
    public void CompiledHandlers_ResolveBothEventTypeNameAlias_AndDefaultClrName()
    {
        var aliased = new ProjectionDescriptor
        {
            ProjectionType = "aliased",
            HandlerType = typeof(AliasedNameProjection),
            ViewType = typeof(GlobalIndexView),
            Version = 1
        };
        var defaultNamed = new ProjectionDescriptor
        {
            ProjectionType = "default",
            HandlerType = typeof(GlobalTagIndexProjection),
            ViewType = typeof(GlobalIndexView),
            Version = 1
        };

        var aliasedMap = aliased.GetCompiledHandlers();
        var defaultMap = defaultNamed.GetCompiledHandlers();

        Assert.True(aliasedMap.ContainsKey(typeof(AliasedNamedEvent)));
        Assert.Equal("delivery-alias", EventTypeNameResolver.GetName(typeof(AliasedNamedEvent)));
        Assert.NotEqual(typeof(AliasedNamedEvent).FullName, EventTypeNameResolver.GetName(typeof(AliasedNamedEvent)));

        Assert.True(defaultMap.ContainsKey(typeof(GlobalTagged)));
        Assert.Equal("test-projection-fixtures.global-tagged", EventTypeNameResolver.GetName(typeof(GlobalTagged)));
        Assert.NotEqual(typeof(GlobalTagged).FullName, EventTypeNameResolver.GetName(typeof(GlobalTagged)));
    }

    private static EventMetadata Meta() =>
        new() { Timestamp = DateTime.UtcNow, UserId = "step2" };

    private static async Task WaitForRunnerCursorAsync(
        LightweightProjectionRunnerService runner,
        long minSequence,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!cts.IsCancellationRequested)
        {
            if (runner.GetObservabilitySnapshot().LastAppliedSequence >= minSequence)
                return;
            try
            {
                await Task.Delay(10, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        throw new TimeoutException(
            $"Runner cursor {runner.GetObservabilitySnapshot().LastAppliedSequence} did not reach {minSequence}.");
    }
}

[EventTypeName("delivery-alias")]
internal sealed record AliasedNamedEvent(Guid Id, DateTime Timestamp) : IEvent;

[GlobalProjection("AliasedNameProbe", tenantScope: TenantScope.SystemGlobal)]
internal sealed class AliasedNameProjection : ProjectionBase<GlobalIndexView>
{
    public void Handle(AliasedNamedEvent _) => State.TotalTags++;
}
