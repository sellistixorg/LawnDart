using Microsoft.Extensions.Options;
using LawnDart.Aggregates;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Tagging;

namespace LawnDart.EventSourcing.Tests.Tagging;

public class AggregateFallbackTagTests
{
    [Fact]
    public async Task FlagOff_StoresClrTypeName_AndHidesThatEventFromTheFormattedTag()
    {
        var store = new InMemoryEventStore();
        var id = Guid.NewGuid();
        var clr = $"ProbeAggregate:{id:D}";
        var formatted = LowercaseTagFormat.Instance.Tag("ProbeAggregate", id);

        await SaveAsync(store, id, formatFallback: false);

        var tags = (await store.ReadStreamAsync(Stream(id)))[0].Tags;
        Assert.Contains(clr, tags);
        Assert.DoesNotContain(formatted, tags);
        Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(clr)))).Events);
        Assert.Empty((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(formatted)))).Events);
    }

    [Fact]
    public async Task FlagOn_StoresFormattedTag_AndHidesAlreadyStoredClrTags()
    {
        var store = new InMemoryEventStore();
        var oldId = Guid.NewGuid();
        var newId = Guid.NewGuid();
        await SaveAsync(store, oldId, formatFallback: false);
        await SaveAsync(store, newId, formatFallback: true, format: LowercaseTagFormat.Instance);

        var oldClr = $"ProbeAggregate:{oldId:D}";
        var newFormatted = LowercaseTagFormat.Instance.Tag("ProbeAggregate", newId);
        var newTags = (await store.ReadStreamAsync(Stream(newId)))[0].Tags;

        Assert.Contains(newFormatted, newTags);
        Assert.DoesNotContain($"ProbeAggregate:{newId:D}", newTags);
        Assert.Empty((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(newFormatted.Replace(newId.ToString("D"), oldId.ToString("D")))))).Events);
        Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(oldClr)))).Events);
        Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(newFormatted)))).Events);
    }

    [Fact]
    public async Task FlagOn_WithoutAFormat_UsesLowercaseTagFormat()
    {
        var store = new InMemoryEventStore();
        var id = Guid.NewGuid();

        await SaveAsync(store, id, formatFallback: true, format: null);

        var tags = (await store.ReadStreamAsync(Stream(id)))[0].Tags;
        Assert.Contains(LowercaseTagFormat.Instance.Tag("ProbeAggregate", id), tags);
    }

    [Fact]
    public async Task FlagOn_StringId_UsesTheStringOverload()
    {
        var store = new InMemoryEventStore();
        var repository = Repository(store, formatFallback: true, format: LowercaseTagFormat.Instance);
        var aggregate = new ProbeAggregate();
        aggregate.SetStreamId("acct:ProbeAggregate:seat-12");
        aggregate.Raise(new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow));

        await repository.SaveAsync(aggregate, new CommandMetadata());

        var tags = (await store.ReadStreamAsync("acct:ProbeAggregate:seat-12"))[0].Tags;
        Assert.Contains("probeaggregate:seat-12", tags);
        Assert.DoesNotContain("ProbeAggregate:seat-12", tags);
    }

    [Fact]
    public async Task ProviderTag_DoesNotSuppressTheClrFallback()
    {
        var store = new InMemoryEventStore();
        var id = Guid.NewGuid();
        var repository = Repository(
            store,
            formatFallback: false,
            format: LowercaseTagFormat.Instance,
            tagProvider: new StudentTagProvider());
        var aggregate = new ProbeAggregate();
        aggregate.SetStreamId(Stream(id));
        aggregate.Raise(new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow));

        await repository.SaveAsync(aggregate, new CommandMetadata());

        var tags = (await store.ReadStreamAsync(Stream(id)))[0].Tags;
        Assert.Contains($"student:{id:D}", tags);
        Assert.Contains($"ProbeAggregate:{id:D}", tags);
    }

    private static string Stream(Guid id) => $"acct:ProbeAggregate:{id:D}";

    private static Task SaveAsync(InMemoryEventStore store, Guid id, bool formatFallback, ITagFormat? format = null)
    {
        var repository = Repository(store, formatFallback, format);
        var aggregate = new ProbeAggregate();
        aggregate.SetStreamId(Stream(id));
        aggregate.Raise(new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow));
        return repository.SaveAsync(aggregate, new CommandMetadata());
    }

    private static AggregateRepository Repository(
        InMemoryEventStore store,
        bool formatFallback,
        ITagFormat? format,
        ITagProvider? tagProvider = null)
        => new(
            store,
            new DefaultMetadataProvider(),
            new NullTenant(),
            Options.Create(new LawnDartOptions { RequireTenantId = false }),
            tagProvider,
            authorizationService: null,
            logger: null,
            snapshotStore: null,
            strategyResolver: null,
            snapshotWriteQueue: null,
            tagFormat: format,
            eventSourcingOptions: Options.Create(new EventSourcingOptions
            {
                FormatAggregateFallbackTag = formatFallback
            }));

    private sealed class NullTenant : ITenantContextProvider
    {
        public string? GetTenantId() => null;
        public string GetTenantIdRequired() => throw new InvalidOperationException();
    }

    private sealed class StudentTagProvider : ITagProvider
    {
        public IEnumerable<string> GetTags(IEvent @event, object? context = null)
        {
            var id = context is AggregateRoot aggregate
                ? StreamIdParser.ExtractAggregateIdString(aggregate.StreamId)
                : null;
            return id is null ? [] : [$"student:{id}"];
        }
    }

    private sealed class ProbeState : IState;

    private sealed class ProbeAggregate : AggregateRoot<ProbeState>
    {
        public void Raise(IEvent @event) => Apply(@event);
        protected override void ApplyEventToState(IEvent @event) { }
    }

    [EventTypeName("aggregate-fallback-tag.probe")]
    private sealed record ProbeEvent(Guid Id, DateTime Timestamp) : IEvent;
}
