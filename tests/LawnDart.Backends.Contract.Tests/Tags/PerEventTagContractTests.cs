using System.Text;
using LawnDart;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Every store writes each event with only its own tags. One append call with several
/// events stays one atomic write, and the typed and log appends store the same tags.
/// </summary>
public abstract class PerEventTagContractTests : ContractSuite
{
    protected PerEventTagContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task StreamAppend_EachEventKeepsOnlyItsOwnTags()
    {
        var handle = await OpenAsync();
        var (a, b, shared) = (Tag("a"), Tag("b"), Tag("shared"));
        var stream = Stream();

        await handle.Store.AppendAsync(stream, [
            new TaggedEvent(ContractEvent.Tick(), a, shared),
            new TaggedEvent(ContractEvent.Tick(), b, shared),
        ]);

        await AssertOwnTagsAsync(handle, stream, a, b, shared);
    }

    [Fact]
    public async Task ConditionalAppend_EachEventKeepsOnlyItsOwnTags()
    {
        var handle = await OpenAsync();
        var (a, b, shared) = (Tag("a"), Tag("b"), Tag("shared"));
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(shared)));

        var result = await handle.Store.AppendAsync(
            [
                new TaggedEvent(ContractEvent.Tick(), a, shared),
                new TaggedEvent(ContractEvent.Tick(), b, shared),
            ],
            condition);

        var events = await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(shared)));
        Assert.Equal(result.SequencePositions, events.Events.Select(e => e.SequencePosition));
        Assert.Equal([a, shared], events.Events[0].Tags);
        Assert.Equal([b, shared], events.Events[1].Tags);

        Assert.Equal(result.SequencePositions[0] + 1, result.SequencePositions[1]);
        await AssertOwnTagsAsync(handle, null, a, b, shared);
    }

    [Fact]
    public async Task TagsOfOneEvent_DoNotMatchAQueryForAnotherEvent()
    {
        var handle = await OpenAsync();
        var (a, b) = (Tag("a"), Tag("b"));
        await handle.Store.AppendAsync(Stream(), [
            new TaggedEvent(ContractEvent.Tick(), a),
            new TaggedEvent(ContractEvent.Tick(), b),
        ]);

        // No single event has both tags, so an item that needs both matches nothing.
        Assert.Empty((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a, b)))).Events);
        Assert.Single((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a)))).Events);
        Assert.Single((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(b)))).Events);

        var both = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(a, b)));
        await handle.Store.AppendAsync([new TaggedEvent(ContractEvent.Tick(), Tag("other"))], both);
        var onlyB = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(b)));
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => handle.Store.AppendAsync([new TaggedEvent(ContractEvent.Tick(), Tag("other"))], onlyB));
    }

    [Fact]
    public async Task EventWithNoTags_IsStoredWithNoTags_BesideATaggedEvent()
    {
        var handle = await OpenAsync();
        var a = Tag("a");
        var stream = Stream();

        await handle.Store.AppendAsync(stream, [
            new TaggedEvent(ContractEvent.Tick()),
            new TaggedEvent(ContractEvent.Tick(), a),
        ]);

        var read = await handle.Store.ReadStreamAsync(stream);
        Assert.Equal(2, read.Count);
        Assert.Empty(read[0].Tags);
        Assert.Equal([a], read[1].Tags);

        var logRead = await handle.Log.ReadStreamAsync(stream);
        Assert.Empty(logRead[0].Tags);
        Assert.Equal([a], logRead[1].Tags);
    }

    [Fact]
    public async Task UntaggedExtension_WritesEventsWithNoTags()
    {
        var handle = await OpenAsync();
        var stream = Stream();
        await handle.Store.AppendAsync(stream, [ContractEvent.Tick(), ContractEvent.Tick()]);
        var read = await handle.Store.ReadStreamAsync(stream);
        Assert.Equal(2, read.Count);
        Assert.All(read, e => Assert.Empty(e.Tags));
    }

    [Fact]
    public async Task ThreeEvents_OneCall_IsOneAtomicWrite()
    {
        var handle = await OpenAsync();
        var (a, b, c, fence) = (Tag("a"), Tag("b"), Tag("c"), Tag("fence"));
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(fence)));

        var result = await handle.Store.AppendAsync(
            [
                new TaggedEvent(ContractEvent.Tick(), a, fence),
                new TaggedEvent(ContractEvent.Tick(), b),
                new TaggedEvent(ContractEvent.Tick(), c),
            ],
            condition);

        Assert.Equal(3, result.SequencePositions.Count);
        var read = (await handle.Store.ReadByQueryAsync(Query.All(), fromSequencePosition: result.SequencePositions[0])).Events;
        Assert.Equal(3, read.Count);
        Assert.Equal(result.SequencePositions[0] + 2, result.SequencePositions[2]);
        Assert.Equal(result.SequencePositions, read.Select(e => e.SequencePosition));
        Assert.Equal([a, fence], read[0].Tags);
        Assert.Equal([b], read[1].Tags);
        Assert.Equal([c], read[2].Tags);

        // The same condition now fails, and none of the three new events is written.
        var (d, e, f) = (Tag("d"), Tag("e"), Tag("f"));
        await Assert.ThrowsAsync<ConcurrencyException>(() => handle.Store.AppendAsync(
            [
                new TaggedEvent(ContractEvent.Tick(), d),
                new TaggedEvent(ContractEvent.Tick(), e),
                new TaggedEvent(ContractEvent.Tick(), f),
            ],
            condition));
        Assert.Empty((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(d)))).Events);
        Assert.Empty((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(e)))).Events);
        Assert.Empty((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(f)))).Events);
    }

    [Fact]
    public async Task ThreeEvents_StreamAppend_WrongVersion_WritesNone()
    {
        var handle = await OpenAsync();
        var stream = Stream();
        var (a, b, c) = (Tag("a"), Tag("b"), Tag("c"));
        await handle.Store.AppendAsync(stream, [new TaggedEvent(ContractEvent.Tick(), a)]);

        await Assert.ThrowsAsync<ConcurrencyException>(() => handle.Store.AppendAsync(
            stream,
            [
                new TaggedEvent(ContractEvent.Tick(), b),
                new TaggedEvent(ContractEvent.Tick(), c),
                new TaggedEvent(ContractEvent.Tick(), a),
            ],
            expectedVersion: 7));

        Assert.Single(await handle.Store.ReadStreamAsync(stream));
        Assert.Empty((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(b)))).Events);
        Assert.Empty((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(c)))).Events);
    }

    [Fact]
    public async Task TypedAndLogAppends_OfTheSameBatch_StoreTheSameTags()
    {
        var handle = await OpenAsync();
        var (a, b, shared) = (Tag("a"), Tag("b"), Tag("shared"));
        var typedStream = Stream();
        var logStream = Stream();

        await handle.Store.AppendAsync(typedStream, [
            new TaggedEvent(ContractEvent.Tick(), a, shared, a),
            new TaggedEvent(ContractEvent.Tick()),
            new TaggedEvent(ContractEvent.Tick(), b),
        ]);
        await handle.Log.AppendAsync(logStream, [
            LogEvent(a, shared, a),
            LogEvent(),
            LogEvent(b),
        ]);

        var typed = await handle.Log.ReadStreamAsync(typedStream);
        var log = await handle.Log.ReadStreamAsync(logStream);
        Assert.Equal(3, typed.Count);
        Assert.Equal(log.Select(e => e.Tags.ToArray()), typed.Select(e => e.Tags.ToArray()));
        Assert.Equal([a, shared], typed[0].Tags);
        Assert.Empty(typed[1].Tags);
        Assert.Equal([b], typed[2].Tags);
    }

    [Fact]
    public async Task RepeatedTagInsideATaggedEvent_IsStoredOnce()
    {
        var handle = await OpenAsync();
        var (a, b) = (Tag("a"), Tag("b"));
        var stream = Stream();

        await handle.Store.AppendAsync(stream, [
            new TaggedEvent(ContractEvent.Tick(), b, a, b, a),
            new TaggedEvent(ContractEvent.Tick(), a, a),
        ]);

        var read = await handle.Store.ReadStreamAsync(stream);
        Assert.Equal([b, a], read[0].Tags);
        Assert.Equal([a], read[1].Tags);
        Assert.Equal(2, (await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a)))).Events.Count);
    }

    [Fact]
    public async Task DcbRepository_AppendWithContext_EachEventKeepsOnlyItsOwnTags()
    {
        var handle = await OpenAsync();
        var repository = new DcbRepository(
            handle.Store,
            new TestMetadataProvider(),
            new TestTenantContextProvider("test-tenant"),
            Options.Create(new LawnDartOptions()),
            null,
            null,
            null);
        var (a, b, shared) = (Tag("a"), Tag("b"), Tag("shared"));

        await repository.AppendWithContextAsync([
            new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 1), a, shared),
            new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 2), b, shared),
        ]);

        var read = (await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(shared)))).Events;
        Assert.Equal(2, read.Count);
        Assert.Contains(a, read[0].Tags);
        Assert.DoesNotContain(b, read[0].Tags);
        Assert.Contains(b, read[1].Tags);
        Assert.DoesNotContain(a, read[1].Tags);
        Assert.Empty((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a, b)))).Events);
    }

    private static async Task AssertOwnTagsAsync(ContractEventStore handle, string? stream, string a, string b, string shared)
    {
        IReadOnlyList<SequencedEvent> byStream = stream is null
            ? (await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(shared)))).Events
            : await handle.Store.ReadStreamAsync(stream);
        Assert.Equal(2, byStream.Count);
        Assert.Equal([a, shared], byStream[0].Tags);
        Assert.Equal([b, shared], byStream[1].Tags);

        var byA = Assert.Single((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a)))).Events);
        Assert.Equal(byStream[0].SequencePosition, byA.SequencePosition);
        Assert.Equal([a, shared], byA.Tags);
        var byB = Assert.Single((await handle.Store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(b)))).Events);
        Assert.Equal(byStream[1].SequencePosition, byB.SequencePosition);
        Assert.Equal([b, shared], byB.Tags);

        IReadOnlyList<RecordedEvent> logRead = stream is null
            ? (await handle.Log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(shared)))).Events
            : await handle.Log.ReadStreamAsync(stream);
        Assert.Equal(2, logRead.Count);
        Assert.Equal([a, shared], logRead[0].Tags);
        Assert.Equal([b, shared], logRead[1].Tags);
        var logByA = Assert.Single((await handle.Log.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(a)))).Events);
        Assert.Equal([a, shared], logByA.Tags);
    }

    private async Task<ContractEventStore> OpenAsync()
        => await Backend.OpenEventStoreAsync(ContractStores.Isolated());

    private static AppendEvent LogEvent(params string[] tags)
        => new("per-event-tag.event", Encoding.UTF8.GetBytes("{}"), tags: tags);

    private static string Stream() => "per-event-tag:" + Guid.NewGuid().ToString("N");

    private static string Tag(string kind) => kind + ":" + Guid.NewGuid().ToString("N");
}
