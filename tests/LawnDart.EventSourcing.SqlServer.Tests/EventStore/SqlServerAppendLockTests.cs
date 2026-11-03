using System.Text;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.TestUtilities;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.EventStore;

/// <summary>
/// The named append locks: which locks an append asks for, and the condition
/// type name look-up after them.
/// </summary>
public sealed class SqlServerAppendLockPlanTests
{
    private const string Scope = "dbo\u001fEvents";

    [Fact]
    public void PlainAppend_TakesStoreShared_AndWrittenTagsShared_StoreFirst_TagsSorted()
    {
        var plan = SqlServerEventStore.BuildAppendLockPlan(
            Scope, [Event("b:1", "a:1"), Event("c:1", "a:1")], condition: null, tagThreshold: 64);

        Assert.Equal(4, plan.Count);
        Assert.Equal(SqlServerEventStore.StoreLockName(Scope), plan[0].Resource);
        Assert.All(plan, l => Assert.False(l.Exclusive));
        var tagNames = plan.Skip(1).Select(l => l.Resource).ToList();
        Assert.Equal(tagNames.OrderBy(n => n, StringComparer.Ordinal), tagNames);
        Assert.Equal(3, tagNames.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void ConditionTags_AreExclusive_OtherWrittenTagsShared_StoreShared()
    {
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("course:1", "student:1")));
        var plan = SqlServerEventStore.BuildAppendLockPlan(
            Scope, [Event("course:1", "student:1", "extra:1")], condition, tagThreshold: 64);

        Assert.False(plan[0].Exclusive);
        Assert.True(Mode(plan, "course:1"));
        Assert.True(Mode(plan, "student:1"));
        Assert.False(Mode(plan, "extra:1"));
        Assert.Equal(4, plan.Count);
    }

    [Fact]
    public void TypeAndTagItem_DoesNotNeedTheStoreExclusive()
    {
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTypeAndTags(["T"], ["course:1"])));
        var plan = SqlServerEventStore.BuildAppendLockPlan(Scope, [Event("course:1")], condition, 64);
        Assert.False(plan[0].Exclusive);
        Assert.True(Mode(plan, "course:1"));
    }

    [Theory]
    [InlineData("types")]
    [InlineData("mixed")]
    [InlineData("all")]
    [InlineData("emptytags")]
    public void ItemWithoutTags_TakesTheStoreExclusive(string shape)
    {
        var query = shape switch
        {
            "types" => Query.FromItems(QueryItem.ByType("T")),
            "mixed" => Query.FromItems(QueryItem.ByTags("course:1"), QueryItem.ByType("T")),
            "emptytags" => Query.FromItems(QueryItem.ByTags("")),
            _ => Query.All()
        };
        var plan = SqlServerEventStore.BuildAppendLockPlan(
            Scope, [Event("course:1")], AppendCondition.FailIfMatches(query), 64);
        Assert.Equal(SqlServerEventStore.StoreLockName(Scope), plan[0].Resource);
        Assert.True(plan[0].Exclusive);
    }

    [Fact]
    public void MoreTagsThanTheThreshold_TakeOnlyTheStoreExclusive()
    {
        var tags = Enumerable.Range(0, 65).Select(i => "t:" + i).ToArray();
        var plan = SqlServerEventStore.BuildAppendLockPlan(Scope, [Event(tags)], null, tagThreshold: 64);
        var only = Assert.Single(plan);
        Assert.True(only.Exclusive);
        Assert.Equal(SqlServerEventStore.StoreLockName(Scope), only.Resource);

        var atLimit = SqlServerEventStore.BuildAppendLockPlan(Scope, [Event(tags[..64])], null, tagThreshold: 64);
        Assert.Equal(65, atLimit.Count);
        Assert.False(atLimit[0].Exclusive);
    }

    [Fact]
    public void ThresholdCounts_ConditionAndWrittenTagsTogether()
    {
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("c:1", "c:2")));
        var plan = SqlServerEventStore.BuildAppendLockPlan(Scope, [Event("w:1", "w:2")], condition, tagThreshold: 3);
        Assert.True(Assert.Single(plan).Exclusive);
    }

    [Fact]
    public void LockNames_StayShort_AndDependOnStoreAndTag()
    {
        var longTag = new string('x', 256);
        var name = SqlServerEventStore.TagLockName(Scope, longTag);
        Assert.True(name.Length <= 255);
        Assert.StartsWith(SqlServerEventStore.AppendLockPrefix, name);
        Assert.True(SqlServerEventStore.StoreLockName(Scope).Length <= 255);

        Assert.NotEqual(name, SqlServerEventStore.TagLockName("other\u001fEvents", longTag));
        Assert.NotEqual(SqlServerEventStore.StoreLockName(Scope), SqlServerEventStore.StoreLockName("other\u001fEvents"));
        Assert.NotEqual(SqlServerEventStore.TagLockName(Scope, "A:1"), SqlServerEventStore.TagLockName(Scope, "a:1"));
        Assert.Equal(name, SqlServerEventStore.TagLockName(Scope, longTag));
    }

    [Fact]
    public void ThresholdBelowOne_IsRejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SqlServerEventStore(
            "Server=localhost;Database=x;User Id=sa;Password=x;TrustServerCertificate=true",
            options: new SqlServerEventStoreOptions { AppendLockTagThreshold = 0 }));
    }

    private static bool Mode(IReadOnlyList<SqlServerEventStore.AppendLock> plan, string tag)
        => plan.Single(l => l.Resource == SqlServerEventStore.TagLockName(Scope, tag)).Exclusive;

    private static AppendEvent Event(params string[] tags)
        => new("T", Encoding.UTF8.GetBytes("{}"), tags: tags);
}

[Trait("Category", "Integration")]
public sealed class SqlServerAppendLockTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private string _connectionString = "";

    public async Task InitializeAsync()
    {
        _container = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025)
                .WithPassword("Test123!")
                .Build());
        _connectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

    /// <summary>
    /// The checker looks up a type name that is not stored yet. Another store object
    /// then stores the first event of that type before the checker takes its locks.
    /// The checker looks the name up again after the locks, so it sees that event.
    /// </summary>
    [Fact]
    public async Task TypeNameFirstStoredBeforeTheLocks_IsSeenByTheCheck()
    {
        var schema = "lk" + Guid.NewGuid().ToString("N")[..8];
        var checker = Store(schema);
        var writer = Store(schema);
        await checker.InitializeSchemaAsync();
        await writer.InitializeSchemaAsync();

        var type = "first.type." + Guid.NewGuid().ToString("N");
        var after = await checker.GetCurrentSequenceAsync();
        var ran = 0;
        checker.BeforeAppendLocks = async ct =>
        {
            if (Interlocked.Increment(ref ran) == 1)
                await writer.AppendAsync("s-" + Guid.NewGuid().ToString("N"), [new AppendEvent(type, Encoding.UTF8.GetBytes("{}"))], cancellationToken: ct);
        };

        // The checker writes another type, so its own write does not store the name first.
        await Assert.ThrowsAsync<ConcurrencyException>(() => checker.AppendAsync(
            [new AppendEvent("other.type", Encoding.UTF8.GetBytes("{}"))],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType(type)), after)));
        Assert.Equal(1, ran);
    }

    [Fact]
    public async Task AppendLocks_AreReleasedAtCommitAndRollback()
    {
        var schema = "lk" + Guid.NewGuid().ToString("N")[..8];
        var store = Store(schema);
        await store.InitializeSchemaAsync();
        var tag = "course:" + Guid.NewGuid().ToString("N");
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType("T")));

        await store.AppendAsync([new AppendEvent("T", Encoding.UTF8.GetBytes("{}"), tags: [tag])], condition);
        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            store.AppendAsync([new AppendEvent("T", Encoding.UTF8.GetBytes("{}"), tags: [tag])], condition));

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            "SELECT COUNT(*) FROM sys.dm_tran_locks WHERE resource_type = 'APPLICATION'", connection);
        Assert.Equal(0, Convert.ToInt32(await command.ExecuteScalarAsync()));
    }

    private SqlServerEventStore Store(string schema)
        => new(
            _connectionString,
            options: new SqlServerEventStoreOptions { SchemaName = schema, RequireTenantId = false });
}
