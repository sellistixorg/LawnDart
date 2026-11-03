using System.Text;
using LawnDart;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// A tag repeated on one event is stored once. The first copy of each tag stays,
/// in order. Tags compare exactly, so a tag that differs only in case is its own tag.
/// </summary>
public abstract class RepeatedTagContractTests : ContractSuite
{
    protected RepeatedTagContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task LogStreamAppend_RepeatedTags_StoredOnce_FirstOrderKept()
    {
        var log = await OpenLogAsync();
        var (a, b, c) = (Tag("a"), Tag("b"), Tag("c"));
        var stream = Stream();
        await log.AppendAsync(stream, [Event(c, a, c, b, a)]);

        var read = Assert.Single(await log.ReadStreamAsync(stream));
        Assert.Equal([c, a, b], read.Tags);
        var byTag = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a)));
        Assert.Equal([c, a, b], Assert.Single(byTag.Events).Tags);
    }

    [Fact]
    public async Task LogConditionalAppend_RepeatedTags_StoredOnce_AndStillMatch()
    {
        var log = await OpenLogAsync();
        var (a, b) = (Tag("a"), Tag("b"));
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(a)));
        await log.AppendAsync([Event(a, b, a, b)], condition);

        var read = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a)));
        Assert.Equal([a, b], Assert.Single(read.Events).Tags);
        await Assert.ThrowsAsync<ConcurrencyException>(() => log.AppendAsync([Event(a)], condition));
    }

    [Fact]
    public async Task TypedAppend_RepeatedTags_StoredOnce()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var (a, b) = (Tag("a"), Tag("b"));
        var stream = Stream();
        await store.AppendAsync(stream, [ContractEvent.Tick(), ContractEvent.Tick()], tags: [a, b, a]);
        var events = await store.ReadStreamAsync(stream);
        Assert.Equal(2, events.Count);
        Assert.All(events, e => Assert.Equal([a, b], e.Tags));

        var conditional = await store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(b)), events[^1].SequencePosition),
            tags: [b, b]);
        var landed = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(b)), fromSequencePosition: conditional.SequencePositions[0]);
        Assert.Equal([b], Assert.Single(landed.Events).Tags);
    }

    [Fact]
    public async Task DcbRepository_RepeatedTags_StoredOnce()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = new DcbRepository(
            store,
            new TestMetadataProvider(),
            new TestTenantContextProvider("test-tenant"),
            Options.Create(new LawnDartOptions()),
            null,
            null,
            null);
        var (a, b) = (Tag("a"), Tag("b"));
        await repository.AppendWithContextAsync([new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 1)], [a, b, a, b]);
        var read = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a)));
        var evt = Assert.Single(read.Events);
        Assert.Equal(evt.Tags.Count, evt.Tags.Distinct(StringComparer.Ordinal).Count());
        Assert.Contains(a, evt.Tags);
        Assert.Contains(b, evt.Tags);
    }

    [Fact]
    public async Task TagsDifferingInCase_StayTwoTags()
    {
        var log = await OpenLogAsync();
        var id = Guid.NewGuid().ToString("N");
        var upper = "Course:" + id;
        var lower = "course:" + id;
        var stream = Stream();
        await log.AppendAsync(stream, [Event(upper, lower, upper)]);

        Assert.Equal([upper, lower], Assert.Single(await log.ReadStreamAsync(stream)).Tags);
        Assert.Single((await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(upper)))).Events);
        Assert.Single((await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(lower)))).Events);
        Assert.Single((await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(upper, lower)))).Events);
    }

    [Fact]
    public async Task QueryItemWithRepeatedTags_ReadsTheSameAsWithout()
    {
        var log = await OpenLogAsync();
        var (a, b) = (Tag("a"), Tag("b"));
        await log.AppendAsync(Stream(), [Event(a)]);
        await log.AppendAsync(Stream(), [Event(a, b)]);

        var once = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a, b)));
        var twice = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a, b, a, b)));
        Assert.Equal(once.Events.Select(e => e.SequencePosition), twice.Events.Select(e => e.SequencePosition));
        Assert.Single(twice.Events);

        var single = await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a, a)));
        Assert.Equal(2, single.Events.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConditionWithRepeatedTags_BehavesTheSameAsWithout(bool repeated)
    {
        var log = await OpenLogAsync();
        var type = "repeat.type." + Guid.NewGuid().ToString("N");
        var (a, b) = (Tag("a"), Tag("b"));
        string[] tags = repeated ? [a, b, a, b] : [a, b];
        var head = await log.GetCurrentSequenceAsync();

        // Only tag a exists: the item needs both tags, so nothing matches.
        await log.AppendAsync(Stream(), [new AppendEvent(type, Encoding.UTF8.GetBytes("{}"), tags: [a])]);
        var byTags = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tags)), head);
        var byTypeAndTags = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTypeAndTags([type], tags)), head);
        await log.AppendAsync([Event(Tag("other"))], byTags);
        await log.AppendAsync([Event(Tag("other"))], byTypeAndTags);

        // An event with both tags now matches.
        await log.AppendAsync(Stream(), [new AppendEvent(type, Encoding.UTF8.GetBytes("{}"), tags: [b, a])]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => log.AppendAsync([Event(Tag("other"))], byTags));
        await Assert.ThrowsAsync<ConcurrencyException>(() => log.AppendAsync([Event(Tag("other"))], byTypeAndTags));
    }

    [Fact]
    public async Task ConcurrentAppends_WithRepeatedConditionTags_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        var a = Tag("a");
        var head = await log.GetCurrentSequenceAsync();
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(a, a)), head);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            await gate.Task;
            try
            {
                await log.AppendAsync([Event(a, a)], condition);
                return (Exception?)null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        })).ToArray();
        gate.SetResult();
        var results = await Task.WhenAll(tasks);
        Assert.All(results, e => Assert.True(e is null or ConcurrencyException, e?.ToString()));
        Assert.Equal(1, results.Count(e => e is null));
        Assert.Equal([a], Assert.Single((await log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a)))).Events).Tags);
    }

    private async Task<IEventLog> OpenLogAsync()
        => (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;

    private static AppendEvent Event(params string[] tags)
        => new("repeat.event", Encoding.UTF8.GetBytes("{}"), tags: tags);

    private static string Stream() => "repeat:" + Guid.NewGuid().ToString("N");

    private static string Tag(string kind) => kind + ":" + Guid.NewGuid().ToString("N");
}

[Collection(InMemoryContractCollection.Name)]
[Trait("Category", "Contract")]
public sealed class InMemoryRepeatedTagContractTests : RepeatedTagContractTests
{
    public InMemoryRepeatedTagContractTests(InMemoryContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("SqlServerContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class SqlServerRepeatedTagContractTests : RepeatedTagContractTests
{
    public SqlServerRepeatedTagContractTests(SqlServerContractFixture fixture) : base(fixture.Backend) { }
}

[Collection("PostgresContract")]
[Trait("Category", "Integration")]
[Trait("Category", "Contract")]
public sealed class PostgresRepeatedTagContractTests : RepeatedTagContractTests
{
    public PostgresRepeatedTagContractTests(PostgresContractFixture fixture) : base(fixture.Backend) { }
}
