using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.Dcb;

/// <summary>
/// Proves keyed <see cref="IDcbSnapshotStore"/> reaches <see cref="IDcbRepository"/>
/// (the factory used to call unkeyed <c>GetService</c> and miss the registration).
/// </summary>
public class DcbRepositoryKeyedSnapshotStoreTests
{
    [Fact]
    public async Task KeyedIDcbSnapshotStore_IsUsedByDcbRepository()
    {
        var recording = new RecordingStore();
        var resolver = new SnapshotStrategyResolver();
        resolver.RegisterForDcb<TestDcbEntity>(new EventCountSnapshotStrategy(1));

        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider("test-tenant"));
        services.Configure<LawnDartOptions>(_ => { });
        services.AddSingleton<ISnapshotStrategyResolver>(resolver);
        services.AddBoundedContext("ordering").UseInMemory();
        services.AddKeyedSingleton<IDcbSnapshotStore>("ordering", recording);

        await using var sp = services.BuildServiceProvider();
        var repo = sp.GetRequiredKeyedService<IDcbRepository>("ordering");

        var entity = await repo.CreateEntityAsync<TestDcbEntity>(["product:sku-keyed"]);
        await repo.HandleCommandAsync(entity, new TestCommand { Value = "one" }, new CommandMetadata { UserId = "u" });

        await recording.WaitForSaveAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(1, recording.SaveCount);
        Assert.Equal(DcbSnapshotId.FromLoadTags(["product:sku-keyed"]), recording.LastDcbId);
    }

    private sealed class RecordingStore : IDcbSnapshotStore
    {
        private int _saves;
        private readonly TaskCompletionSource<bool> _saved = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int SaveCount => Volatile.Read(ref _saves);
        public string? LastDcbId { get; private set; }

        public Task<(TState? State, SnapshotInfo? Info)> LoadDcbSnapshotAsync<TState>(
            string dcbId, CancellationToken ct = default)
            => Task.FromResult<(TState?, SnapshotInfo?)>((default, null));

        public Task SaveDcbSnapshotAsync<TState>(
            string dcbId,
            long globalSequence,
            TState state,
            byte[]? consistencyMarker = null,
            IReadOnlyList<string>? loadTags = null,
            CancellationToken ct = default)
        {
            LastDcbId = dcbId;
            Interlocked.Increment(ref _saves);
            _saved.TrySetResult(true);
            return Task.CompletedTask;
        }

        public async Task WaitForSaveAsync(TimeSpan timeout)
        {
            var finished = await Task.WhenAny(_saved.Task, Task.Delay(timeout)).ConfigureAwait(false);
            if (finished != _saved.Task)
                throw new TimeoutException("Keyed DCB snapshot save was not observed.");
            await _saved.Task.ConfigureAwait(false);
        }
    }
}
