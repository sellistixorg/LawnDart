using System.Text;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// Concurrent conditional appends. Each race starts 16 appends at the same moment.
/// When every append's condition matches every other append's event, exactly one
/// may succeed and the rest must fail with <see cref="ConcurrencyException"/>.
/// </summary>
/// <remarks>
/// Filler events written before a race give the racers different <c>after</c>
/// values that sit on different rows. Fillers never match the racers' condition.
/// Every test runs on its own store, so type names and tags are its own.
/// </remarks>
public abstract class DcbAppendRaceContractTests : ContractSuite
{
    private const int Racers = 16;
    private const int Rounds = 10;
    private const int Fillers = 40;

    protected DcbAppendRaceContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    // R1
    [Fact(Timeout = 120_000)]
    public async Task R01_TypesOnly_SameAfter_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var type = TypeName("r1");
            var head = await log.GetCurrentSequenceAsync();
            var query = Query.FromItems(QueryItem.ByType(type));
            await RaceAsync(log, query, _ => head, _ => Event(type));
        }
    }

    // R2
    [Fact(Timeout = 120_000)]
    public async Task R02_TypesOnly_DifferentAfter_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var type = TypeName("r2");
            var fillers = await WriteFillersAsync(log, _ => Event(TypeName("filler")));
            var query = Query.FromItems(QueryItem.ByType(type));
            await RaceAsync(log, query, i => fillers[i * 2], _ => Event(type));
        }
    }

    // R3
    [Fact(Timeout = 120_000)]
    public async Task R03_TypeAndTag_DifferentAfter_FillerHasTagWithOtherType_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var type = TypeName("r3");
            var tag = Tag("course");
            var otherType = TypeName("filler");
            var fillers = await WriteFillersAsync(log, _ => Event(otherType, tag));
            var query = Query.FromItems(QueryItem.ByTypeAndTags([type], [tag]));
            await RaceAsync(log, query, i => fillers[i * 2], _ => Event(type, tag));
        }
    }

    // R4
    [Fact(Timeout = 120_000)]
    public async Task R04_OwnTagItem_OrTypesOnlyItem_SameAfter_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var type = TypeName("r4");
            var head = await log.GetCurrentSequenceAsync();
            var tags = Enumerable.Range(0, Racers).Select(_ => Tag("student")).ToArray();
            await RaceAsync(
                log,
                i => Query.FromItems(QueryItem.ByTags(tags[i]), QueryItem.ByType(type)),
                _ => head,
                i => Event(type, tags[i]),
                verify: Query.FromItems(QueryItem.ByType(type)));
        }
    }

    // R5
    [Fact(Timeout = 120_000)]
    public async Task R05_TagsOnly_DifferentAfter_FillerOnOtherTag_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var tag = Tag("course");
            var other = Tag("course");
            var fillers = await WriteFillersAsync(log, _ => Event(TypeName("filler"), other));
            var query = Query.FromItems(QueryItem.ByTags(tag));
            await RaceAsync(log, query, i => fillers[i * 2], _ => Event(TypeName("r5"), tag));
        }
    }

    // R6
    [Fact(Timeout = 120_000)]
    public async Task R06_TypeAndTag_SameAfter_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var type = TypeName("r6");
            var tag = Tag("course");
            var head = await log.GetCurrentSequenceAsync();
            var query = Query.FromItems(QueryItem.ByTypeAndTags([type], [tag]));
            await RaceAsync(log, query, _ => head, _ => Event(type, tag));
        }
    }

    // R7
    [Fact(Timeout = 120_000)]
    public async Task R07_TwoTags_DifferentAfter_FillerHasOneTag_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var a = Tag("course");
            var b = Tag("student");
            var fillers = await WriteFillersAsync(log, _ => Event(TypeName("filler"), a));
            var query = Query.FromItems(QueryItem.ByTags(a, b));
            await RaceAsync(log, query, i => fillers[i * 2], _ => Event(TypeName("r7"), a, b));
        }
    }

    // R8
    [Fact(Timeout = 120_000)]
    public async Task R08_TwoTags_DifferentAfter_FillersCarryEachTagAlone_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var a = Tag("course");
            var b = Tag("student");
            var fillers = await WriteFillersAsync(log, i => Event(TypeName("filler"), i % 2 == 0 ? a : b));
            var query = Query.FromItems(QueryItem.ByTags(a, b));
            await RaceAsync(log, query, i => fillers[i], _ => Event(TypeName("r8"), a, b));
        }
    }

    // R9. A match-all query matches the fillers too, so every racer reads after the head.
    [Fact(Timeout = 120_000)]
    public async Task R09_MatchAll_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            await WriteFillersAsync(log, _ => Event(TypeName("filler")));
            var head = await log.GetCurrentSequenceAsync();
            await RaceAsync(log, Query.All(), _ => head, _ => Event(TypeName("r9")));
        }
    }

    // R10
    [Fact(Timeout = 120_000)]
    public async Task R10_Checker_AgainstPlainStreamAppends_SeesEveryEarlierMatch()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var type = TypeName("r10");
            var tag = Tag("course");
            var fillers = await WriteFillersAsync(log, _ => Event(TypeName("filler"), tag));
            var after = fillers[0];
            var query = Query.FromItems(QueryItem.ByTypeAndTags([type], [tag]));
            await CheckerAgainstWritersAsync(log, query, after, () => Event(type, tag));
        }
    }

    // R11
    [Fact(Timeout = 120_000)]
    public async Task R11_Checker_OnNeverStoredType_AgainstFirstWriters_SeesEveryEarlierMatch()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var type = TypeName("r11");
            var after = await log.GetCurrentSequenceAsync();
            var query = Query.FromItems(QueryItem.ByType(type));
            // The checker writes another type, so its own write does not store the name first.
            await CheckerAgainstWritersAsync(log, query, after, () => Event(type), () => Event(TypeName("checker")));
        }
    }

    // R12
    [Fact(Timeout = 120_000)]
    public async Task R12_OppositeTagOrders_FinishWithoutDeadlockErrors()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var a = Tag("zeta");
            var b = Tag("alpha");
            var head = await log.GetCurrentSequenceAsync();
            await RaceAsync(
                log,
                i => Query.FromItems(QueryItem.ByTags(i % 2 == 0 ? [a, b] : [b, a])),
                _ => head,
                i => Event(TypeName("r12"), i % 2 == 0 ? [a, b] : [b, a]),
                verify: Query.FromItems(QueryItem.ByTags(a, b)));
        }
    }

    [Fact(Timeout = 120_000)]
    public async Task R12b_ReversedTagLists_MixedPlainAndConditional_FinishWithoutDeadlockErrors()
    {
        var log = await OpenLogAsync();
        var tags = Enumerable.Range(0, 10).Select(i => Tag("t" + i)).ToArray();
        var reversed = tags.Reverse().ToArray();
        for (var round = 0; round < Rounds; round++)
        {
            var head = await log.GetCurrentSequenceAsync();
            var results = await RunBarrierAsync(Racers, i => i % 2 == 0
                ? log.AppendAsync(Stream(), [Event(TypeName("plain"), i % 4 == 0 ? tags : reversed)])
                : log.AppendAsync(
                    [Event(TypeName("checked"), i % 4 == 1 ? tags : reversed)],
                    AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tags[i % 10], tags[(i + 3) % 10])), head)));
            Assert.All(results, e => Assert.True(e is null or ConcurrencyException, e?.ToString()));
            Assert.Equal(Racers / 2, results.Where((_, i) => i % 2 == 0).Count(e => e is null));
        }
    }

    // More distinct tags than the per-tag lock limit (64 by default).
    [Fact(Timeout = 120_000)]
    public async Task ManyTags_AboveLockLimit_ExactlyOneWins()
    {
        var log = await OpenLogAsync();
        for (var round = 0; round < 3; round++)
        {
            var tags = Enumerable.Range(0, 70).Select(i => Tag("many" + i)).ToArray();
            var head = await log.GetCurrentSequenceAsync();
            var query = Query.FromItems(QueryItem.ByTags(tags));

            // Every racer writes all 70 tags, and they are new this round, so one tag
            // finds the events that landed. This test is about the append locks. A read
            // with 70 tags in one item is left out: on Postgres it takes many seconds
            // to plan.
            await RaceAsync(
                log,
                _ => query,
                _ => head,
                _ => Event(TypeName("many"), tags),
                verify: Query.FromItems(QueryItem.ByTags(tags[0])));
        }
    }

    [Fact(Timeout = 120_000)]
    public async Task DisjointTags_AllWin()
    {
        var log = await OpenLogAsync();
        var head = await log.GetCurrentSequenceAsync();
        var tags = Enumerable.Range(0, Racers).Select(_ => Tag("own")).ToArray();
        var results = await RunBarrierAsync(Racers, i => log.AppendAsync(
            [Event(TypeName("own"), tags[i])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tags[i])), head)));
        Assert.All(results, e => Assert.Null(e));
    }

    // One at a time: a logic error shows up here, a locking error only in the races.
    [Fact]
    public async Task OneAtATime_RaceShapes_SecondAppendFails()
    {
        var log = await OpenLogAsync();
        var type = TypeName("seq");
        var tag = Tag("course");
        var a = Tag("course");
        var b = Tag("student");
        var cases = new (Query Query, Func<AppendEvent> Event)[]
        {
            (Query.FromItems(QueryItem.ByType(type)), () => Event(type)),
            (Query.FromItems(QueryItem.ByTypeAndTags([type], [tag])), () => Event(type, tag)),
            (Query.FromItems(QueryItem.ByTags(Tag("own")), QueryItem.ByType(type)), () => Event(type, Tag("own"))),
            (Query.FromItems(QueryItem.ByTags(a, b)), () => Event(TypeName("seq"), a, b)),
            (Query.All(), () => Event(TypeName("seq")))
        };

        foreach (var (query, makeEvent) in cases)
        {
            await WriteFillersAsync(log, i => Event(TypeName("filler"), i % 2 == 0 ? a : b), count: 4);
            var after = await log.GetCurrentSequenceAsync();
            await log.AppendAsync([makeEvent()], AppendCondition.FailIfMatches(query, after));
            await Assert.ThrowsAsync<ConcurrencyException>(
                () => log.AppendAsync([makeEvent()], AppendCondition.FailIfMatches(query, after)));
        }
    }

    [ContractFact(Timeout = 60_000)]
    public async Task PausedAppend_DoesNotHoldUpAppendOnOtherTags()
    {
        SkipUnless(Cap.CanPauseBeforeCommit && Cap.AppendLocksPerTag, "Needs a pause before commit and per-tag append locks.");

        // This checks the append locks only. On SQL Server two other waits are left out:
        // the stream registry's tag upkeep, and, with locking reads, the condition check
        // reading past the paused append's new rows. Neither is an append lock.
        var opened = await Backend.OpenEventStoreAsync(new ContractStoreOptions
        {
            SchemaName = "c" + Guid.NewGuid().ToString("N"),
            EnableRegistry = false,
            ReadCommittedSnapshot = true
        });
        var log = opened.Log;
        var held = Tag("held");
        var free = Tag("free");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var paused = log.AppendAsync(
            [Event(TypeName("held"), held)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(held))));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        try
        {
            await log.AppendAsync(
                    [Event(TypeName("free"), free)],
                    AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(free))))
                .WaitAsync(TimeSpan.FromSeconds(15));
            await log.AppendAsync(Stream(), [Event(TypeName("free"), free)]).WaitAsync(TimeSpan.FromSeconds(15));
        }
        finally
        {
            release.TrySetResult();
        }

        await paused;
    }

    [ContractFact(Timeout = 60_000)]
    public async Task PausedAppend_HoldsUpTypesOnlyChecker_ThenCheckerSeesIt()
    {
        SkipUnless(Cap.CanPauseBeforeCommit && Cap.AppendLocksPerTag, "Needs a pause before commit and per-tag append locks.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var log = opened.Log;
        var type = TypeName("typed");
        var after = await log.GetCurrentSequenceAsync();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var paused = log.AppendAsync(Stream(), [Event(type, Tag("any"))]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        var checker = log.AppendAsync(
            [Event(type)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType(type)), after));
        await Task.Delay(500);
        Assert.False(checker.IsCompleted, "A types only condition must wait for an append in flight.");

        release.TrySetResult();
        await paused;
        await Assert.ThrowsAsync<ConcurrencyException>(() => checker);
    }

    [ContractFact(Timeout = 60_000)]
    public async Task PausedAppend_AboveTagLockLimit_HoldsUpEveryAppend()
    {
        SkipUnless(Cap.CanPauseBeforeCommit && Cap.AppendLocksPerTag, "Needs a pause before commit and per-tag append locks.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var log = opened.Log;
        var many = Enumerable.Range(0, 70).Select(i => Tag("many" + i)).ToArray();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var paused = log.AppendAsync(Stream(), [Event(TypeName("many"), many)]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        var other = log.AppendAsync(Stream(), [Event(TypeName("other"), Tag("other"))]);
        await Task.Delay(500);
        Assert.False(other.IsCompleted, "An append above the tag lock limit takes the whole store.");

        release.TrySetResult();
        await paused;
        await other;
    }

    private async Task<IEventLog> OpenLogAsync()
        => (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Log;

    private static Task RaceAsync(
        IEventLog log,
        Query query,
        Func<int, long?> after,
        Func<int, AppendEvent> makeEvent)
        => RaceAsync(log, _ => query, after, makeEvent, query);

    private static async Task RaceAsync(
        IEventLog log,
        Func<int, Query> query,
        Func<int, long?> after,
        Func<int, AppendEvent> makeEvent,
        Query verify)
    {
        var start = await log.GetCurrentSequenceAsync();
        var results = await RunBarrierAsync(Racers, i => log.AppendAsync(
            [makeEvent(i)],
            AppendCondition.FailIfMatches(query(i), after(i))));
        Assert.All(results, e => Assert.True(e is null or ConcurrencyException, e?.ToString()));
        Assert.Equal(1, results.Count(e => e is null));
        Assert.Equal(Racers - 1, results.Count(e => e is ConcurrencyException));
        var landed = await log.ReadByQueryAsync(verify, fromSequencePosition: start + 1);
        Assert.Single(landed.Events);
    }

    /// <summary>
    /// One conditional append races 15 plain stream appends that each write a matching event.
    /// If the checker wins at position P, no matching event may sit between <paramref name="after"/> and P.
    /// </summary>
    private static async Task CheckerAgainstWritersAsync(
        IEventLog log,
        Query query,
        long after,
        Func<AppendEvent> makeEvent,
        Func<AppendEvent>? makeCheckerEvent = null)
    {
        long? won = null;
        var results = await RunBarrierAsync(Racers, async i =>
        {
            if (i == Racers / 2)
            {
                var result = await log.AppendAsync([(makeCheckerEvent ?? makeEvent)()], AppendCondition.FailIfMatches(query, after));
                won = result.SequencePositions[0];
            }
            else
            {
                await log.AppendAsync(Stream(), [makeEvent()]);
            }
        });

        Assert.All(results, e => Assert.True(e is null or ConcurrencyException, e?.ToString()));
        Assert.Equal(Racers - 1, results.Where((_, i) => i != Racers / 2).Count(e => e is null));
        if (won is long position)
        {
            var matches = await log.ReadByQueryAsync(query, fromSequencePosition: after + 1);
            Assert.DoesNotContain(matches.Events, e => e.SequencePosition < position);
        }
    }

    private static async Task<long[]> WriteFillersAsync(IEventLog log, Func<int, AppendEvent> makeEvent, int count = Fillers)
    {
        var positions = new long[count];
        for (var i = 0; i < count; i++)
            positions[i] = (await log.AppendAsync(Stream(), [makeEvent(i)])).SequencePositions[0];
        return positions;
    }

    private static async Task<Exception?[]> RunBarrierAsync(int racers, Func<int, Task> work)
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = 0;
        var tasks = Enumerable.Range(0, racers).Select(i => Task.Run(async () =>
        {
            if (Interlocked.Increment(ref ready) == racers)
                gate.SetResult();
            await gate.Task.ConfigureAwait(false);
            try
            {
                await work(i).ConfigureAwait(false);
                return (Exception?)null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        })).ToArray();
        return await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private static AppendEvent Event(string type, params string[] tags)
        => new(type, Encoding.UTF8.GetBytes("{}"), tags: tags);

    private static string TypeName(string kind) => "race." + kind + "." + Guid.NewGuid().ToString("N");

    private static string Stream() => "race:" + Guid.NewGuid().ToString("N");

    private static string Tag(string kind) => kind + ":" + Guid.NewGuid().ToString("N");
}
