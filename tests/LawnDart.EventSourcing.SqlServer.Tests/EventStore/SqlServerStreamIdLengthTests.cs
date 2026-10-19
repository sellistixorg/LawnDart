using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventSourcing.SqlServer.Snapshots;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

public class SqlServerStreamIdLengthTests
{
    private const string UnusedConnection =
        "Server=127.0.0.1,9;Database=unused;User ID=unused;Password=unused;TrustServerCertificate=True;Connect Timeout=1";

    [Fact]
    public void LikeParameter_EscapesMetacharacters()
    {
        Assert.Equal(@"acme\_west:%", StreamIdPrefixSql.LikeParameter("acme_west:"));
        Assert.Equal(@"acme\%west:%", StreamIdPrefixSql.LikeParameter("acme%west:"));
        Assert.Equal(@"acme\[west:%", StreamIdPrefixSql.LikeParameter("acme[west:"));
        Assert.Equal(@"a\\b%", StreamIdPrefixSql.LikeParameter(@"a\b"));
        var predicate = StreamIdPrefixSql.Predicate("StreamId");
        Assert.DoesNotContain("COLLATE", predicate, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("StreamId LIKE @Prefix ESCAPE '\\'", predicate);
    }

    [Fact]
    public void EnsureStreamId_Accepts255_Rejects256()
    {
        SqlServerColumnLimits.EnsureStreamId(new string('a', SqlServerColumnLimits.StreamId), "streamId");

        var ex = Assert.Throws<ArgumentException>(() =>
            SqlServerColumnLimits.EnsureStreamId(new string('a', SqlServerColumnLimits.StreamId + 1), "streamId"));
        Assert.Contains("255", ex.Message);
        Assert.Equal("streamId", ex.ParamName);
    }

    [Fact]
    public async Task AppendAsync_StreamIdLongerThanColumn_ThrowsBeforeSql()
    {
        var store = new SqlServerEventStore(UnusedConnection);
        var streamId = new string('a', SqlServerColumnLimits.StreamId + 1);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            store.AppendAsync(streamId, [new AppendEvent("length-check", new byte[] { 1 })]));

        Assert.Contains("255", ex.Message);
        Assert.Equal(nameof(streamId), ex.ParamName);
    }

    [Fact]
    public async Task SaveSnapshotAsync_StreamIdLongerThanColumn_ThrowsBeforeSql()
    {
        var store = new SqlServerSnapshotStore(new SqlServerEventStoreOptions
        {
            ConnectionString = UnusedConnection
        });
        var streamId = new string('b', SqlServerColumnLimits.StreamId + 1);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            store.SaveSnapshotAsync(streamId, version: 1, globalSequence: 1, state: 1));

        Assert.Contains("255", ex.Message);
        Assert.Contains("EventSnapshots", ex.Message);
    }
}
