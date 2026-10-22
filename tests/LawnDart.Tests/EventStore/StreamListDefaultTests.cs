using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public class StreamListDefaultTests
{
    [Fact]
    public async Task DefaultImplementation_SortsAndPages()
    {
        var registry = new ListingFake();
        registry.Items.Add(Stream("acme:Order:b", "Order", "red"));
        registry.Items.Add(Stream("acme:Order:A", "Order", "red"));
        registry.Items.Add(Stream("acme:Customer:1", "Customer", "red"));
        registry.Items.Add(Stream("acme:Order:a", "Order", "blue"));

        IStreamRegistry surface = registry;
        var filter = new StreamListFilter { Prefix = "acme:Order:", Tag = "red" };
        var first = await surface.ListStreamsAsync(filter, pageSize: 1);
        Assert.Equal("acme:Order:A", Assert.Single(first.Streams).StreamId);
        Assert.NotNull(first.NextPageToken);

        var second = await surface.ListStreamsAsync(filter, first.NextPageToken, pageSize: 1);
        Assert.Equal("acme:Order:b", Assert.Single(second.Streams).StreamId);
        Assert.Null(second.NextPageToken);
    }

    [Fact]
    public async Task DefaultImplementation_RejectsABadTokenAndADifferentFilter()
    {
        IStreamRegistry surface = new ListingFake();
        await Assert.ThrowsAsync<ArgumentException>(() => surface.ListStreamsAsync(pageToken: "@@@"));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => surface.ListStreamsAsync(pageSize: 0));

        var registry = new ListingFake();
        registry.Items.Add(Stream("acme:Order:A", "Order", "red"));
        registry.Items.Add(Stream("acme:Order:b", "Order", "red"));
        surface = registry;
        var issued = await surface.ListStreamsAsync(new StreamListFilter { Tag = "red" }, pageSize: 1);
        var token = StreamPageToken.Encode("acme:Order:A", new StreamListFilter { Tag = "red" });
        Assert.Equal(token, issued.NextPageToken);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            surface.ListStreamsAsync(new StreamListFilter { Tag = "blue" }, issued.NextPageToken));

        var past = StreamPageToken.Encode("acme:Order:b", new StreamListFilter { Tag = "red" });
        var empty = await surface.ListStreamsAsync(new StreamListFilter { Tag = "red" }, past);
        Assert.Empty(empty.Streams);
        Assert.Null(empty.NextPageToken);
    }

    private static StreamMetadata Stream(string id, string aggregateType, string tag)
        => new()
        {
            StreamId = id,
            AggregateType = aggregateType,
            Tags = [tag],
            Status = StreamStatus.Active
        };

    private sealed class ListingFake : IStreamRegistry
    {
        public List<StreamMetadata> Items { get; } = [];

        public Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default)
            => Task.FromResult(Items.FirstOrDefault(item => item.StreamId == streamId));

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(string aggregateType, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(string tag, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(string? prefix = null, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(Items
                .Select(item => item.StreamId)
                .Where(id => prefix is null || id.StartsWith(prefix, StringComparison.Ordinal))
                .ToList());

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(long afterSequencePosition, int? limit = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }
}
