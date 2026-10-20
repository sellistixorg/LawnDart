using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Repository saves use the same tenant id check as a direct append.
/// </summary>
public abstract class RepositoryTenantContractTests : ContractSuite
{
    protected RepositoryTenantContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task SaveAsync_ReservedTenantDefault_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true))).Store;
        var repository = AggregateRepo(store, "default", validateNames: true);
        var aggregate = Pending("default");

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.SaveAsync(aggregate, new CommandMetadata { TenantId = "default" }));
        Assert.Contains("reserved", ex.Message);
    }

    [Fact]
    public async Task SaveAsync_ValidateNamesFalse_KeepsTenantDefault()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true, validateNames: false))).Store;
        var repository = AggregateRepo(store, "default", validateNames: false);
        var aggregate = Pending("default");

        await repository.SaveAsync(aggregate, new CommandMetadata { TenantId = "default" });

        Assert.Single(await store.ReadStreamAsync(aggregate.StreamId));
    }

    [Fact]
    public async Task AppendWithContextAsync_ReservedTenantDefault_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true))).Store;
        var repository = DcbRepo(store, "default", validateNames: true);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => repository.AppendWithContextAsync(
            [ContractEvent.Tick()],
            ["order:absent"],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("order:absent"))),
            new CommandMetadata { TenantId = "default" }));
        Assert.Contains("reserved", ex.Message);
    }

    [Fact]
    public async Task AppendWithContextAsync_ValidateNamesFalse_KeepsTenantDefault()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true, validateNames: false))).Store;
        var repository = DcbRepo(store, "default", validateNames: false);

        var positions = await repository.AppendWithContextAsync(
            [ContractEvent.Tick()],
            ["order:kept"],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("order:kept"))),
            new CommandMetadata { TenantId = "default" });

        var read = await store.ReadByQueryAsync(Query.All(), fromSequencePosition: positions[0], limit: 1);
        var evt = Assert.Single(read.Events);
        Assert.StartsWith("default:dcb:", evt.StreamId);
    }

    [Fact]
    public async Task AppendWithContextAsync_MissingTenant_WhenRequired_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(requireTenantId: true))).Store;
        var repository = DcbRepo(store, tenantId: null, validateNames: true);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => repository.AppendWithContextAsync(
            [ContractEvent.Tick()],
            ["order:absent"],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("order:absent")))));
        Assert.Contains("TenantId is required in event metadata", ex.Message);
    }

    private static AggregateRepository AggregateRepo(IEventStore store, string tenantId, bool validateNames)
        => new(
            store,
            new TestMetadataProvider(),
            new TestTenantContextProvider(tenantId),
            Options.Create(new LawnDartOptions
            {
                RequireTenantId = true,
                ValidateNames = validateNames
            }));

    private static DcbRepository DcbRepo(IEventStore store, string? tenantId, bool validateNames)
        => new(
            store,
            new TestMetadataProvider(new TestTenantContextProvider(tenantId)),
            new TestTenantContextProvider(tenantId),
            Options.Create(new LawnDartOptions
            {
                RequireTenantId = true,
                ValidateNames = validateNames
            }));

    private static ProbeAggregate Pending(string tenantId)
    {
        var aggregate = new ProbeAggregate();
        aggregate.SetStreamId($"{tenantId}:ProbeAggregate:{Guid.NewGuid()}");
        aggregate.Raise(ContractEvent.Tick());
        return aggregate;
    }

    private sealed class ProbeState : IState;

    private sealed class ProbeAggregate : AggregateRoot<ProbeState>
    {
        public void Raise(IEvent @event) => Apply(@event);

        protected override void ApplyEventToState(IEvent @event)
        {
        }
    }
}
