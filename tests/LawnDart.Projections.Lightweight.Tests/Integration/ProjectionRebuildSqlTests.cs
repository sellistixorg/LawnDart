using Microsoft.Data.SqlClient;
using LawnDart.EventSourcing.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Admin;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;
using Testcontainers.MsSql;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>
/// Rebuild round-trip against SQL stores (Testcontainers).
/// Asserts that <c>ProjectionViews</c> / <c>ProjectionCheckpoints</c> rows are physically
/// deleted, then rebuilt from scratch by <see cref="IProjectionAdmin.RebuildAsync"/>.
/// </summary>
[Collection("SqlRebuild")]
[Trait("Category", "Integration")]
public sealed class ProjectionRebuildSqlTests : IAsyncLifetime
{
    private readonly MsSqlContainer _container;
    private string _cs = string.Empty;

    public ProjectionRebuildSqlTests()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("RebuildTest1!")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _cs = _container.GetConnectionString();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();

    // ── helpers ──────────────────────────────────────────────────────────────

    private static EventMetadata Meta() => new() { Timestamp = DateTime.UtcNow, UserId = "test" };

    private static LightweightProjectionOptions AggressiveOptions => new()
    {
        PollInterval       = TimeSpan.FromMilliseconds(10),
        CheckpointInterval = 1,
        BatchSize          = 50
    };

    private static ProjectionRegistration MakeCounterReg() =>
        ProjectionScanner.Scan([typeof(CounterSummaryProjection).Assembly])
            .First(r => r.StorageKey.Contains("CounterSummary"));

    private async Task<long> CountViewRowsAsync(string storageKey, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_cs);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [dbo].[ProjectionViews] WHERE ProjectionType = @p";
        cmd.Parameters.AddWithValue("@p", storageKey);
        return (long)(int)await cmd.ExecuteScalarAsync(ct)!;
    }

    private async Task<long> CountCheckpointRowsAsync(string storageKey, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_cs);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [dbo].[ProjectionCheckpoints] WHERE ProjectionType = @p";
        cmd.Parameters.AddWithValue("@p", storageKey);
        return (long)(int)await cmd.ExecuteScalarAsync(ct)!;
    }

    // ── Rebuild round-trip ────────────────────────────────────────────────────

    [Fact]
    public async Task RebuildRoundTrip_SqlStores_PhysicalRowsDeletedThenRebuilt()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var reg        = MakeCounterReg();
        var viewStore  = new SqlViewStore(_cs);
        var checkpoints = new SqlCheckpointStore(_cs, viewStore);

        await viewStore.InitializeSchemaAsync(cts.Token);
        await checkpoints.InitializeSchemaAsync(cts.Token);

        var eventStore = new InMemoryEventStore();
        var streamId   = $"Counter:{Guid.NewGuid()}";

        // 1. Seed 5 events
        for (var i = 1; i <= 5; i++)
            await eventStore.AppendAsync(streamId,
                [new CounterIncremented(Guid.NewGuid(), DateTime.UtcNow, i)],
                metadata: Meta(), cancellationToken: cts.Token);

        // 2. Run projection to steady state
        var runner1 = new LightweightProjectionRunnerService(
            reg, eventStore, viewStore, checkpoints, new SingleNodePartitioningService(), AggressiveOptions);
        await runner1.StartAsync(CancellationToken.None);
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, reg.StorageKey, 4, TimeSpan.FromSeconds(30));
        await runner1.StopAsync(cts.Token);
        runner1.Dispose();

        // 3. Assert rows exist in SQL
        var viewRowsBefore      = await CountViewRowsAsync(reg.StorageKey, cts.Token);
        var checkpointRowsBefore = await CountCheckpointRowsAsync(reg.StorageKey, cts.Token);
        Assert.True(viewRowsBefore > 0,       "Expected view rows after first run");
        Assert.True(checkpointRowsBefore > 0, "Expected checkpoint rows after first run");

        // 4. Physically delete using the store APIs (verifies the SQL store implementations)
        await viewStore.DeleteAllViewsAsync(reg.StorageKey, cts.Token);
        await checkpoints.DeleteCheckpointAsync(reg.StorageKey, cancellationToken: cts.Token);

        Assert.Equal(0, await CountViewRowsAsync(reg.StorageKey, cts.Token));
        Assert.Equal(0, await CountCheckpointRowsAsync(reg.StorageKey, cts.Token));

        // 5. Rebuild via IProjectionAdmin (stop→delete→restart), using a fresh runner manager
        var manager = new BoundedContextProjectionRunnerManager(
            [reg],
            r => new LightweightProjectionRunnerService(
                r, eventStore, viewStore, checkpoints, new SingleNodePartitioningService(), AggressiveOptions));

        var admin = new BoundedContextProjectionAdmin(
            manager, checkpoints, viewStore,
            new ProjectionRegistrationCatalog([reg]),
            new SingleNodePartitioningService());

        await admin.RebuildAsync("CounterSummary", ct: cts.Token);

        // 6. Wait for runner (started by RebuildAsync) to reach checkpoint >= 5 (all 5 events)
        await ProjectionRunnerTestHarness.WaitForCheckpointAtLeastAsync(
            checkpoints, reg.StorageKey, 5, TimeSpan.FromSeconds(30));

        // 7. Assert rows physically re-created in SQL
        Assert.True(await CountViewRowsAsync(reg.StorageKey, cts.Token) > 0,
            "Expected view rows after rebuild");
        Assert.True(await CountCheckpointRowsAsync(reg.StorageKey, cts.Token) > 0,
            "Expected checkpoint rows after rebuild");

        // 8. Assert view state is correct (count = 1+2+3+4+5 = 15)
        var viewJson = await viewStore.GetViewAsync(reg.StorageKey, streamId, cts.Token);
        Assert.NotNull(viewJson);
        var view = System.Text.Json.JsonSerializer.Deserialize<CounterView>(viewJson!, ProjectionViewJson.Read);
        Assert.Equal(15, view!.Count);

        await manager.StopAllAsync(cts.Token);
    }
}
