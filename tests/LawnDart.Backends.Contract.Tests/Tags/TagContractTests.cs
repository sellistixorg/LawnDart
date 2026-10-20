using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Tagging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

public abstract class TagContractTests : ContractSuite
{
    protected TagContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task FallbackFlag_Off_StoresClrTypeName()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var id = Guid.NewGuid();
        await SaveAsync(store, id, formatFallback: false);
        var tags = (await store.ReadStreamAsync(Stream(id)))[0].Tags;
        var clr = $"ProbeAggregate:{id:D}";
        var formatted = LowercaseTagFormat.Instance.Tag("ProbeAggregate", id);
        Assert.Contains(clr, tags);
        Assert.DoesNotContain(formatted, tags);
        Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(clr)))).Events);
        Assert.Empty((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(formatted)))).Events);
    }

    [Fact]
    public async Task FallbackFlag_On_StoresFormattedTag_AndLeavesOldRowsUnderTheClrTag()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var oldId = Guid.NewGuid();
        var newId = Guid.NewGuid();
        await SaveAsync(store, oldId, formatFallback: false);
        await SaveAsync(store, newId, formatFallback: true);
        var oldClr = $"ProbeAggregate:{oldId:D}";
        var formatted = LowercaseTagFormat.Instance.Tag("ProbeAggregate", newId);
        var newTags = (await store.ReadStreamAsync(Stream(newId)))[0].Tags;
        Assert.Contains(formatted, newTags);
        Assert.DoesNotContain($"ProbeAggregate:{newId:D}", newTags);
        Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(oldClr)))).Events);
        Assert.Empty((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(
            LowercaseTagFormat.Instance.Tag("ProbeAggregate", oldId))))).Events);
        Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(formatted)))).Events);
    }

    [Fact]
    public async Task Warning_FiresOnce_DoesNotFireWhenDisabled_AndDoesNotChangeRows()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var logger = new TagListLogger();
        var id = Guid.NewGuid();
        var bad = $"Student:{id:D}";
        var good = LowercaseTagFormat.Instance.Tag("student", id);
        var loud = Dcb(store, logger, warn: true);
        await loud.AppendWithContextAsync([new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow)], [bad]);
        var first = await loud.GetStateAsync<CountingState>([bad]);
        var second = await loud.GetStateAsync<CountingState>([bad]);
        var formatted = await loud.GetStateAsync<CountingState>([good]);
        Assert.Equal(1, first.Count);
        Assert.Equal(1, second.Count);
        Assert.Equal(0, formatted.Count);
        Assert.Equal(1, logger.WarningsFor(bad));
        Assert.Equal(0, logger.WarningsFor(good));

        var quietTag = $"Section:{Guid.NewGuid():D}";
        var quiet = Dcb(store, logger, warn: false);
        await quiet.GetStateAsync<CountingState>([quietTag]);
        await quiet.AppendWithContextAsync(
            [new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow)],
            [LowercaseTagFormat.Instance.Tag("section", Guid.NewGuid())],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(quietTag))));
        Assert.Equal(0, logger.WarningsFor(quietTag));
    }

    [Fact]
    public async Task Warning_SkipsTagsTheFormatWouldProduce()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var logger = new TagListLogger();
        var repo = Dcb(store, logger, warn: true);
        var student = LowercaseTagFormat.Instance.Tag("student", Guid.NewGuid());
        var tenant = LowercaseTagFormat.Instance.Tag("tenant", "academy-tenant");
        await repo.AppendWithContextAsync([new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow)], [student, tenant]);
        var state = await repo.GetStateAsync<CountingState>([student, tenant]);
        Assert.Equal(1, state.Count);
        Assert.Empty(logger.Warnings);
    }

    private static DcbRepository Dcb(IEventStore store, TagListLogger logger, bool warn)
        => new(
            store,
            new DefaultMetadataProvider(),
            new NullTenant(),
            Options.Create(new LawnDartOptions { RequireTenantId = false }),
            tagProvider: null,
            authorizationService: null,
            logger,
            Options.Create(new EventSourcing.EventSourcingOptions { WarnOnUnknownTag = warn }),
            dcbSnapshotStore: null,
            strategyResolver: null,
            snapshotWriteQueue: null,
            tagFormat: LowercaseTagFormat.Instance);

    private static async Task SaveAsync(IEventStore store, Guid id, bool formatFallback)
    {
        var repository = new AggregateRepository(
            store,
            new DefaultMetadataProvider(),
            new NullTenant(),
            Options.Create(new LawnDartOptions { RequireTenantId = false }),
            tagProvider: null,
            authorizationService: null,
            logger: null,
            snapshotStore: null,
            strategyResolver: null,
            snapshotWriteQueue: null,
            tagFormat: LowercaseTagFormat.Instance,
            eventSourcingOptions: Options.Create(new EventSourcing.EventSourcingOptions
            {
                FormatAggregateFallbackTag = formatFallback
            }));
        var aggregate = await repository.CreateAsync<ProbeAggregate>(Stream(id));
        aggregate.Raise(new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(aggregate, new CommandMetadata());
    }

    private static string Stream(Guid id) => $"acct:ProbeAggregate:{id:D}";

    private sealed class NullTenant : ITenantContextProvider
    {
        public string? GetTenantId() => null;
        public string GetTenantIdRequired() => throw new InvalidOperationException();
    }

    private sealed class ProbeState : IState;

    private sealed class ProbeAggregate : AggregateRoot<ProbeState>
    {
        public void Raise(IEvent @event) => Apply(@event);
        protected override void ApplyEventToState(IEvent @event) { }
    }

    [EventTypeName("tag-contract.probe")]
    private sealed record ProbeEvent(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class CountingState : IState
    {
        public int Count { get; private set; }
        public void Apply(ProbeEvent @event) => Count++;
    }

    private sealed class TagListLogger : ILogger<DcbRepository>
    {
        public List<string> Warnings { get; } = [];
        public int WarningsFor(string tag) => Warnings.Count(message => message.Contains(tag, StringComparison.Ordinal));
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }
}
