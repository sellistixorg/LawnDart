using System.Text;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// Event ids the store writes: a missing id is minted as UUID version 7, a caller id is
/// kept, and one id copied onto every event of an append is made distinct.
/// </summary>
public abstract class EventIdContractTests : ContractSuite
{
    private const string EventName = "event-id-contract.raw-event";

    protected EventIdContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [ContractFact]
    public async Task StreamAppend_WithoutEventId_MintsVersion7Ids()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var before = NowMilliseconds();
        await opened.Log.AppendAsync(Stream(), [Raw(), Raw(), Raw()]);
        await Task.Delay(5);
        await opened.Log.AppendAsync(Stream(), [Raw()]);
        var after = NowMilliseconds();

        var ids = await StoredIdsAsync(opened);
        Assert.Equal(4, ids.Count);
        Assert.Equal(4, ids.Distinct().Count());
        foreach (var id in ids)
        {
            AssertVersion7(id);
            Assert.InRange(TimeOf(id), before, after);
        }

        Assert.True(TimeOf(ids[2]) < TimeOf(ids[3]), "An id minted later must carry a later time.");
    }

    [ContractFact]
    public async Task DcbAppend_WithoutEventId_MintsVersion7Ids()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var tag = Tag.Parse($"event-id:{Guid.NewGuid():N}");
        var before = NowMilliseconds();
        await opened.Log.AppendAsync(
            [Raw(tag: tag), Raw(tag: tag)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))));
        var after = NowMilliseconds();

        var ids = await StoredIdsAsync(opened);
        Assert.Equal(2, ids.Count);
        Assert.NotEqual(ids[0], ids[1]);
        foreach (var id in ids)
        {
            AssertVersion7(id);
            Assert.InRange(TimeOf(id), before, after);
        }
    }

    [ContractFact]
    public async Task CallerEventIds_AreKept()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var rawVersion4 = Guid.NewGuid();
        var rawVersion7 = Guid.CreateVersion7();
        var typed = ContractEvent.Tick();
        await opened.Log.AppendAsync(Stream(), [Raw(rawVersion4)]);
        await opened.Log.AppendAsync(Stream(), [Raw(rawVersion7)]);
        await opened.Store.AppendAsync(Stream(), [typed], metadata: new EventMetadata());

        Assert.Equal([rawVersion4, rawVersion7, typed.Id], await StoredIdsAsync(opened));
    }

    [ContractFact]
    public async Task CopiedEventId_InOneAppend_GivesDistinctIdsThatKeepVersionAndTime()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var version7 = Guid.CreateVersion7();
        var version4 = Guid.NewGuid();
        await opened.Log.AppendAsync(Stream(), [Raw(version7), Raw(version7), Raw(version7)]);
        await opened.Log.AppendAsync(Stream(), [Raw(version4), Raw(version4)]);

        var ids = await StoredIdsAsync(opened);
        Assert.Equal(5, ids.Count);
        Assert.Equal(5, ids.Distinct().Count());
        Assert.Equal(version7, ids[0]);
        Assert.Equal(version4, ids[3]);
        foreach (var id in ids.Take(3))
        {
            AssertVersion7(id);
            Assert.Equal(TimeOf(version7), TimeOf(id));
        }

        Assert.Equal(4, ids[4].Version);
    }

    private async Task<IReadOnlyList<Guid>> StoredIdsAsync(ContractEventStore opened)
    {
        SkipUnless(opened.ReadStoredEventIdsAsync is not null, "This backend cannot read the stored event ids.");
        return await opened.ReadStoredEventIdsAsync!(CancellationToken.None);
    }

    private static StreamId Stream() => StreamId.For("event-id", Guid.NewGuid().ToString("N"));

    private static AppendEvent Raw(Guid? eventId = null, Tag? tag = null)
    {
        var metadata = eventId is { } id
            ? Encoding.UTF8.GetBytes($$"""{"EventId":"{{id}}"}""")
            : Encoding.UTF8.GetBytes("""{"UserId":"event-id-contract"}""");
        return new AppendEvent(
            EventName,
            Encoding.UTF8.GetBytes("{}"),
            metadata,
            tags: tag is { } t ? [t] : null);
    }

    private static void AssertVersion7(Guid id)
    {
        Assert.Equal(7, id.Version);
        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes, bigEndian: true, out _);
        Assert.Equal(0x80, bytes[8] & 0xC0);
    }

    private static long TimeOf(Guid id)
    {
        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes, bigEndian: true, out _);
        long milliseconds = 0;
        for (var i = 0; i < 6; i++)
            milliseconds = (milliseconds << 8) | bytes[i];
        return milliseconds;
    }

    private static long NowMilliseconds() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
