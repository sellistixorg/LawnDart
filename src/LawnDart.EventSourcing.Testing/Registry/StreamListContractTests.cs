using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.EventSourcing.Testing;

public abstract class StreamListContractTests : ContractSuite
{
    protected StreamListContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task ListStreams_OrdersByStreamId_CharacterByCharacter()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        // B, Z, _, a. A case-insensitive primary key would still keep these as four streams.
        // A culture sort puts "a" before "B". Character order puts "B" first and "_" before "a".
        foreach (var id in new[] { "acme:Order:a", "acme:Order:_", "acme:Order:Z", "acme:Order:B" })
            await AppendAsync(store, id);

        var page = await DrainAsync(store, new StreamListFilter { Prefix = "acme:Order:" }, pageSize: 2);
        Assert.Equal(["acme:Order:B", "acme:Order:Z", "acme:Order:_", "acme:Order:a"], page.Ids);
        Assert.Null(page.LastToken);
    }

    [Fact]
    public async Task ListStreams_WhileAppendsRun_DoesNotSkipOrRepeat()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var prefix = "acme:Order:";
        var initial = new List<string>();
        for (var i = 0; i < 20; i++)
        {
            var id = prefix + i.ToString("D4");
            initial.Add(id);
            await AppendAsync(store, id);
        }

        var appending = Task.Run(async () =>
        {
            for (var i = 0; i < 8; i++)
            {
                await AppendAsync(store, prefix + "m" + i.ToString("D4"));
                await AppendAsync(store, prefix + "-" + i.ToString("D4"));
            }
        });

        var seen = new List<string>();
        string? token = null;
        var filter = new StreamListFilter { Prefix = prefix };
        do
        {
            var page = await store.ListStreamsAsync(filter, token, pageSize: 3);
            seen.AddRange(page.Streams.Select(stream => stream.StreamId).Select(static s => s.ToString()).ToList());
            token = page.NextPageToken;
        }
        while (token is not null);

        await appending;

        Assert.Equal(seen.Count, seen.Distinct(StringComparer.Ordinal).Count());
        for (var i = 1; i < seen.Count; i++)
            Assert.True(string.CompareOrdinal(seen[i - 1], seen[i]) < 0);
        foreach (var id in initial)
            Assert.Contains(id, seen);
    }

    [Fact]
    public async Task ListStreams_FiltersCombine()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await AppendAsync(store, "acme:Order:1", "color:red");
        await AppendAsync(store, "acme:Order:2", "color:blue");
        await AppendAsync(store, "acme:Customer:1", "color:red");
        await AppendAsync(store, "other:Order:1", "color:red");

        var page = await store.ListStreamsAsync(new StreamListFilter
        {
            Prefix = "acme:",
            Tag = Tag.Parse("color:red"),
            AggregateType = "Order"
        });

        Assert.Equal([StreamId.Parse("acme:Order:1")], page.Streams.Select(stream => stream.StreamId).ToArray());
        Assert.Null(page.NextPageToken);
    }

    [Fact]
    public async Task ListStreams_Prefix_TreatsLikeMetacharactersAsLiteral()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var suffix = Guid.NewGuid().ToString("N");
        await AppendAsync(store, $"acme_west{suffix}:Order:1");
        await AppendAsync(store, $"acmeXwest{suffix}:Order:1");

        var page = await store.ListStreamsAsync(new StreamListFilter { Prefix = $"acme_west{suffix}:" });
        Assert.Equal([StreamId.Parse($"acme_west{suffix}:Order:1")], page.Streams.Select(stream => stream.StreamId).ToArray());
    }

    [Fact]
    public async Task ListStreams_DigitPrefix_IsTheSameSetOnEveryPageSize()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var matching = new[]
        {
            "7:Order:2",
            "7:Order:10",
            "7:Order:b",
            "7:Order:01",
            "7:Order:a",
            "7:Order:02",
            "7:Order:c",
            "7:Order:d",
        };
        var digitTag = new HashSet<string>(StringComparer.Ordinal)
        {
            "7:Order:01",
            "7:Order:02",
            "7:Order:2",
            "7:Order:b",
        };
        foreach (var id in matching)
            await AppendAsync(store, id, digitTag.Contains(id) ? ["kind:digit"] : []);

        await AppendAsync(store, "70:Order:1", "kind:digit");
        await AppendAsync(store, "8:Order:1");
        await AppendAsync(store, "a:Order:1", "kind:digit");

        var expected = matching.OrderBy(id => id, StringComparer.Ordinal).ToArray();
        foreach (var pageSize in new[] { 1, 3, 100 })
        {
            var page = await DrainAsync(store, new StreamListFilter { Prefix = "7:" }, pageSize);
            Assert.Equal(expected, page.Ids);
            Assert.Null(page.LastToken);
            Assert.Equal(page.Ids.Count, page.Ids.Distinct(StringComparer.Ordinal).Count());
            Assert.DoesNotContain("70:Order:1", page.Ids);
            Assert.DoesNotContain("8:Order:1", page.Ids);
            Assert.DoesNotContain("a:Order:1", page.Ids);
        }

        var tagged = digitTag.OrderBy(id => id, StringComparer.Ordinal).ToArray();
        foreach (var pageSize in new[] { 1, 2, 50 })
        {
            var page = await DrainAsync(store, new StreamListFilter { Prefix = "7:", Tag = Tag.Parse("kind:digit") }, pageSize);
            Assert.Equal(tagged, page.Ids);
            Assert.Null(page.LastToken);
            Assert.DoesNotContain("70:Order:1", page.Ids);
            Assert.DoesNotContain("a:Order:1", page.Ids);
        }
    }

    [Fact]
    public async Task ListStreams_TagFilter_PagesEveryMatch()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var expected = new List<string>();
        for (var i = 0; i < 25; i++)
        {
            var id = "acme:Order:" + i.ToString("D4");
            expected.Add(id);
            await AppendAsync(store, id, "kind:shared");
        }

        await AppendAsync(store, "acme:Order:other", "kind:lonely");
        expected.Sort(StringComparer.Ordinal);

        var page = await DrainAsync(store, new StreamListFilter { Tag = Tag.Parse("kind:shared") }, pageSize: 7);
        Assert.Equal(expected, page.Ids);
        Assert.DoesNotContain("acme:Order:other", page.Ids);
    }

    [Fact]
    public async Task ListStreams_LaterAppend_AddsTag()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var streamId = "acme:Order:" + Guid.NewGuid().ToString("N");
        await AppendAsync(store, streamId, "kind:alpha");
        await AppendAsync(store, streamId, "kind:beta");

        var alpha = await store.ListStreamsAsync(new StreamListFilter { Tag = Tag.Parse("kind:alpha") });
        var beta = await store.ListStreamsAsync(new StreamListFilter { Tag = Tag.Parse("kind:beta") });
        Assert.Equal(streamId, Assert.Single(alpha.Streams).StreamId.ToString());
        Assert.Equal(streamId, Assert.Single(beta.Streams).StreamId.ToString());
        Assert.Contains("kind:alpha", alpha.Streams[0].Tags.Select(static t => t.ToString()).ToArray());
        Assert.Contains("kind:beta", beta.Streams[0].Tags.Select(static t => t.ToString()).ToArray());
    }

    [Fact]
    public async Task ListStreams_TokenRoundTrip_AndEmptyLastPage()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await AppendAsync(store, "acme:Order:1");
        await AppendAsync(store, "acme:Order:2");
        var filter = new StreamListFilter { Prefix = "acme:Order:" };

        var first = await store.ListStreamsAsync(filter, pageSize: 1);
        Assert.Equal("acme:Order:1", Assert.Single(first.Streams).StreamId.ToString());
        Assert.NotNull(first.NextPageToken);

        var second = await store.ListStreamsAsync(filter, first.NextPageToken, pageSize: 1);
        Assert.Equal("acme:Order:2", Assert.Single(second.Streams).StreamId.ToString());
        Assert.Null(second.NextPageToken);
    }

    [Fact]
    public async Task ListStreams_NoMatches_ReturnsEmptyPage()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await AppendAsync(store, "acme:Order:1", "color:red");
        var page = await store.ListStreamsAsync(new StreamListFilter { Tag = Tag.Parse("tag:missing") });
        Assert.Empty(page.Streams);
        Assert.Null(page.NextPageToken);
    }

    [Fact]
    public async Task ListStreams_BadToken_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await Assert.ThrowsAsync<ArgumentException>(() => store.ListStreamsAsync(pageToken: "@@@"));
        await Assert.ThrowsAsync<ArgumentException>(() => store.ListStreamsAsync(pageToken: "YQ"));
    }

    [Fact]
    public async Task ListStreams_TokenFromAnotherFilter_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await AppendAsync(store, "acme:Order:1", "color:red");
        await AppendAsync(store, "acme:Order:2", "color:red");
        var issued = await store.ListStreamsAsync(new StreamListFilter { Tag = Tag.Parse("color:red") }, pageSize: 1);
        Assert.NotNull(issued.NextPageToken);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.ListStreamsAsync(new StreamListFilter { Tag = Tag.Parse("color:blue") }, issued.NextPageToken));
    }

    [Fact]
    public async Task ListStreams_PageSize_IsBounded()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.ListStreamsAsync(pageSize: 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.ListStreamsAsync(pageSize: -1));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.ListStreamsAsync(pageSize: StreamPage.MaxPageSize + 1));

        var widest = await store.ListStreamsAsync(pageSize: StreamPage.MaxPageSize);
        Assert.Empty(widest.Streams);
        Assert.Null(widest.NextPageToken);

        await AppendAsync(store, "acme:Order:1");
        var one = await store.ListStreamsAsync(pageSize: 1);
        Assert.Single(one.Streams);
        Assert.Null(one.NextPageToken);
    }

    [ContractFact]
    public async Task ListStreams_ArchivedStream_IsOmitted()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var store = opened.Store;
        var streamId = "acme:Order:" + Guid.NewGuid().ToString("N");
        await AppendAsync(store, streamId, "color:red");
        SkipUnless(opened.MarkStreamArchivedAsync is not null, "This backend cannot mark a stream archived.");
        await opened.MarkStreamArchivedAsync!(streamId, CancellationToken.None);

        Assert.NotNull(await store.GetStreamAsync(StreamId.Parse(streamId)));
        Assert.Equal(StreamStatus.Archived, (await store.GetStreamAsync(StreamId.Parse(streamId)))!.Status);
        Assert.Empty((await store.ListStreamsAsync(new StreamListFilter { Prefix = "acme:Order:" })).Streams);
        Assert.Empty(await store.EnumerateStreamIdsAsync("acme:Order:"));
        Assert.Equal(0, await store.GetStreamCountAsync("acme:Order:"));
        Assert.Empty(await store.GetStreamsByTagAsync(Tag.Parse("color:red")));
        Assert.Empty(await store.GetStreamsByAggregateTypeAsync("Order"));
    }

    private static async Task AppendAsync(IEventStore store, string streamId, params string[] tags)
    {
        var id = StreamId.Parse(streamId);
        var tenant = id.TenantId;
        await store.AppendAsync(
            id,
            [new TaggedEvent(ContractEvent.Tick(), tags.Select(Tag.Parse).ToArray())],
            metadata: tenant is null ? null : new EventMetadata { TenantId = tenant });
    }

    private static async Task<(List<string> Ids, string? LastToken)> DrainAsync(
        IEventStore store,
        StreamListFilter filter,
        int pageSize)
    {
        var ids = new List<string>();
        string? token = null;
        string? last = null;
        do
        {
            var page = await store.ListStreamsAsync(filter, token, pageSize);
            ids.AddRange(page.Streams.Select(stream => stream.StreamId).Select(static s => s.ToString()).ToList());
            last = page.NextPageToken;
            token = page.NextPageToken;
        }
        while (token is not null);

        return (ids, last);
    }
}
