using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.Snapshots;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.Snapshots;

public class EventSourcingRepositoriesTests
{
    [Fact]
    public void AddSnapshotWriteInfrastructure_IsIdempotent()
    {
        var services = new ServiceCollection();
        services.AddSnapshotWriteInfrastructure();
        services.AddSnapshotWriteInfrastructure();

        using var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetService<ISnapshotWriteQueue>());
        Assert.Single(sp.GetServices<IHostedService>().OfType<SnapshotWriteHostedService>());
    }

    [Fact]
    public async Task CreateAggregateRepository_EnqueuesSnapshotThroughWriteQueue()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(o =>
        {
            o.RequireTenantId = false;
            o.EnableAuthorization = false;
        });
        services.AddBoundedContext("default").UseInMemory().WithEventTypes(typeof(Ticked));
        services.AddSingleton<ISnapshotStrategyResolver>(_ =>
        {
            var resolver = new SnapshotStrategyResolver();
            resolver.RegisterForAggregate<Probe>(new EventCountSnapshotStrategy(1));
            return resolver;
        });

        using var sp = services.BuildServiceProvider();
        var pump = new SnapshotWriteHostedService(sp.GetRequiredService<ISnapshotWriteQueue>());
        await pump.StartAsync(CancellationToken.None);

        try
        {
            var repository = EventSourcingRepositories.CreateAggregateRepository(sp, "default");
            var id = Guid.NewGuid();
            var aggregate = await repository.GetOrCreateAsync<Probe>(id);
            await repository.HandleCommandAsync(aggregate, new Tick(Guid.NewGuid()));

            var store = sp.GetRequiredService<ISnapshotStore>();
            await WaitUntilAsync(async () =>
            {
                var (_, info) = await store.LoadSnapshotAsync<ProbeState>($"Probe:{id}");
                return info is not null;
            });
        }
        finally
        {
            await pump.StopAsync(CancellationToken.None);
        }
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return;
            await Task.Delay(25);
        }

        throw new TimeoutException("Snapshot was not written through the public repository factory.");
    }

    private sealed record Tick(Guid Id) : ICommand;

    [EventTypeName("contract-factory.tick")]
    private sealed record Ticked(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class ProbeState : IState
    {
        public int Count { get; set; }
    }

    private sealed class Probe : AggregateRoot<ProbeState>
    {
        public void Handle(Tick _) => Apply(new Ticked(Guid.NewGuid(), DateTime.UtcNow));

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is Ticked)
                State.Count++;
        }
    }
}
