using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public class StreamListDefaultTests
{
    [Fact]
    public async Task DefaultImplementation_SortsAndPages()
    {
        var registry = new ListingFake();
        registry.Items.Add(Stream("acme:Order:b", "Order", "color:red"));
        registry.Items.Add(Stream("acme:Order:A", "Order", "color:red"));
        registry.Items.Add(Stream("acme:Customer:1", "Customer", "color:red"));
        registry.Items.Add(Stream("acme:Order:a", "Order", "color:blue"));

        IStreamRegistry surface = registry;
        var filter = new StreamListFilter { Prefix = "acme:Order:", Tag = Tag.Parse("color:red") };
        var first = await surface.ListStreamsAsync(filter, pageSize: 1);
        Assert.Equal("acme:Order:A", Assert.Single(first.Streams).StreamId.ToString());
        Assert.NotNull(first.NextPageToken);

        var second = await surface.ListStreamsAsync(filter, first.NextPageToken, pageSize: 1);
        Assert.Equal("acme:Order:b", Assert.Single(second.Streams).StreamId.ToString());
        Assert.Null(second.NextPageToken);
    }

    [Fact]
    public async Task DefaultImplementation_RejectsABadTokenAndADifferentFilter()
    {
        IStreamRegistry surface = new ListingFake();
        await Assert.ThrowsAsync<ArgumentException>(() => surface.ListStreamsAsync(pageToken: "@@@"));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => surface.ListStreamsAsync(pageSize: 0));

        var registry = new ListingFake();
        registry.Items.Add(Stream("acme:Order:A", "Order", "color:red"));
        registry.Items.Add(Stream("acme:Order:b", "Order", "color:red"));
        surface = registry;
        var issued = await surface.ListStreamsAsync(new StreamListFilter { Tag = Tag.Parse("color:red") }, pageSize: 1);
        var token = StreamPageToken.Encode("acme:Order:A", new StreamListFilter { Tag = Tag.Parse("color:red") });
        Assert.Equal(token, issued.NextPageToken);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            surface.ListStreamsAsync(new StreamListFilter { Tag = Tag.Parse("color:blue") }, issued.NextPageToken));

        var past = StreamPageToken.Encode("acme:Order:b", new StreamListFilter { Tag = Tag.Parse("color:red") });
        var empty = await surface.ListStreamsAsync(new StreamListFilter { Tag = Tag.Parse("color:red") }, past);
        Assert.Empty(empty.Streams);
        Assert.Null(empty.NextPageToken);
    }

    private static StreamMetadata Stream(string id, string aggregateType, string tag)
        => new()
        {
            StreamId = StreamId.Parse(id),
            AggregateType = aggregateType,
            Tags = [Tag.Parse(tag)],
            Status = StreamStatus.Active
        };

    private sealed class ListingFake : IStreamRegistry
    {
        public List<StreamMetadata> Items { get; } = [];

        public Task<StreamMetadata?> GetStreamAsync(StreamId streamId, CancellationToken cancellationToken = default)
            => Task.FromResult(Items.FirstOrDefault(item => item.StreamId == streamId));

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(string aggregateType, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(Tag tag, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<StreamId>> EnumerateStreamIdsAsync(string? prefix = null, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<StreamId>>(Items
                .Select(item => item.StreamId)
                .Where(id => prefix is null || id.ToString().StartsWith(prefix, StringComparison.Ordinal))
                .ToList());

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(long afterSequencePosition, int? limit = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }
}
