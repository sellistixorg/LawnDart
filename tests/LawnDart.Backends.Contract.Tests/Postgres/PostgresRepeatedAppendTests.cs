using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventStore;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Postgres does not keep event ids unique. Two attempts of one append (same event
/// ids, same time), as a lost reply through a pooler can produce, are two appends
/// unless the append has an expected version or a condition. Those checks are what
/// make a retry safe: the second attempt fails them and writes nothing.
/// </summary>
[Collection("PostgresContract")]
[Trait("Category", "Integration")]
public sealed class PostgresRepeatedAppendTests
{
    private readonly PostgresContractFixture _fixture;

    public PostgresRepeatedAppendTests(PostgresContractFixture fixture) => _fixture = fixture;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnconditionalRepeat_IsSavedTwice(bool dcb)
    {
        var (first, second) = await OpenPairAsync(Tenancy.Shared);
        var stream = dcb ? StreamId.Dcb("acme") : StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));
        var events = Events(2);

        var a = await first.Store.AppendAsync(stream, events);
        var b = await second.Store.AppendAsync(stream, events);

        Assert.NotEqual(a.SequencePositions, b.SequencePositions);
        Assert.Equal(4, await CountByIdsAsync(first.SchemaName, events));
    }

    [Fact]
    public async Task RepeatWithAnExpectedVersion_IsRejected()
    {
        var (first, second) = await OpenPairAsync(Tenancy.Shared);
        var stream = StreamId.For("acme", "Order", Guid.NewGuid().ToString("N"));
        var events = Events(2);

        await first.Store.AppendAsync(stream, events, expectedVersion: -1);
        await Assert.ThrowsAnyAsync<ConcurrencyException>(() => second.Store.AppendAsync(stream, events, expectedVersion: -1));

        Assert.Equal(2, await CountByIdsAsync(first.SchemaName, events));
    }

    [Fact]
    public async Task RepeatWithACondition_IsRejected()
    {
        var (first, second) = await OpenPairAsync(Tenancy.Single);
        var tag = Tag.Parse("repeat:" + Guid.NewGuid().ToString("N"));
        var events = Events(2, tag);
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag)), await first.Log.GetCurrentSequenceAsync());

        await first.Store.AppendAsync(events, condition);
        await Assert.ThrowsAnyAsync<ConcurrencyException>(() => second.Store.AppendAsync(events, condition));

        Assert.Equal(2, await CountByIdsAsync(first.SchemaName, events));
    }

    private async Task<(ContractEventStore First, ContractEventStore Second)> OpenPairAsync(Tenancy tenancy)
    {
        var options = ContractStores.Isolated(tenancy: tenancy);
        var backend = (PostgresContractBackend)_fixture.Backend;
        var first = await backend.OpenEventStoreAsync(options);
        var second = await backend.OpenEventStoreAsync(options);
        var time = TruncatedNow();
        Store(first).AppendTimeForTests = time;
        Store(second).AppendTimeForTests = time;
        return (first, second);
    }

    private static TaggedEvent[] Events(int count, Tag? tag = null)
        => Enumerable.Range(0, count)
            .Select(_ => new TaggedEvent(new ContractTestEvent(Guid.CreateVersion7(), DateTime.UtcNow), [tag ?? Tag.Parse("repeat:x")]))
            .ToArray();

    private static DateTime TruncatedNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - (now.Ticks % 10), DateTimeKind.Utc);
    }

    private async Task<long> CountByIdsAsync(string schema, TaggedEvent[] events)
    {
        await using var connection = new NpgsqlConnection(_fixture.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT count(*) FROM \"{schema}\".events WHERE event_id = ANY(@ids)", connection);
        command.Parameters.AddWithValue("ids", events.Select(e => ((ContractTestEvent)e.Event).Id).ToArray());
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static PostgresEventStore Store(ContractEventStore opened) => (PostgresEventStore)opened.Log;
}
