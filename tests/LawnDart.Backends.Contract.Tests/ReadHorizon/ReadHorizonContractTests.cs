using System.Text.RegularExpressions;
using LawnDart.EventStore;
using Microsoft.Extensions.Logging;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// An open append must not let a later commit skip it.
/// Backends without a read horizon skip these tests.
/// </summary>
public abstract class ReadHorizonContractTests : ContractSuite
{
    protected ReadHorizonContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [ContractFact]
    public Task OpenAppend_DoesNotLetALaterCommitSkipIt_SnapshotOff()
        => RunOpenAppend(false);

    [ContractFact]
    public Task OpenAppend_DoesNotLetALaterCommitSkipIt_SnapshotOn()
        => RunOpenAppend(true);

    private async Task RunOpenAppend(bool readCommittedSnapshot)
    {
        SkipUnless(
            Cap.HasReadHorizon && Cap.CanPauseBeforeCommit,
            "In-memory appends commit in the same call and have no read horizon.");

        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(readCommittedSnapshot: readCommittedSnapshot));
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        await using var handle = opened.Subscriptions.Subscribe("ord-sub", fromSequence: 1);
        var first = await ReadExactlyAsync(handle, 1, TimeSpan.FromSeconds(15));
        Assert.Equal(1, first[0].SequencePosition);

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var slow = opened.Store.AppendAsync("slow", [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var fast = await opened.Store.AppendAsync("fast", [ContractEvent.Tick()]);
        var during = await DrainAsync(handle, TimeSpan.FromSeconds(1));
        Assert.DoesNotContain(during, e => e.SequencePosition == fast.SequencePositions[0]);
        if (readCommittedSnapshot)
            Assert.Equal(1, await opened.Store.GetCurrentSequenceAsync());

        release.TrySetResult();
        var slowResult = await slow.WaitAsync(TimeSpan.FromSeconds(15));
        var rest = await ReadExactlyAsync(handle, 2, TimeSpan.FromSeconds(15));
        Assert.Equal(
            new[] { slowResult.SequencePositions[0], fast.SequencePositions[0] },
            rest.Select(e => e.SequencePosition));
        Assert.Equal(fast.SequencePositions[0], await opened.Store.GetCurrentSequenceAsync());
    }

    [ContractFact]
    public Task HeldAppend_LongerThanGrace_IsSkippedWithAWarning_SnapshotOff()
        => RunHeldPastGrace(false);

    [ContractFact]
    public Task HeldAppend_LongerThanGrace_IsSkippedWithAWarning_SnapshotOn()
        => RunHeldPastGrace(true);

    private async Task RunHeldPastGrace(bool readCommittedSnapshot)
    {
        SkipUnless(
            Cap.HasReadHorizon && Cap.CanPauseBeforeCommit,
            "In-memory appends commit in the same call and have no read horizon.");

        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(
            readHorizonGracePeriod: TimeSpan.FromMilliseconds(250),
            readCommittedSnapshot: readCommittedSnapshot));
        var first = await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var slow = opened.Store.AppendAsync("slow", [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var fast = await opened.Store.AppendAsync("fast", [ContractEvent.Tick()]);

        var head = await WaitForHeadAsync(opened.Store, fast.SequencePositions[0], TimeSpan.FromSeconds(5));
        Assert.Equal(fast.SequencePositions[0], head);
        var during = await opened.Log.ReadByQueryAsync(Query.All());
        Assert.Contains(during.Events, e => e.SequencePosition == fast.SequencePositions[0]);
        Assert.DoesNotContain(during.Events, e => e.StreamId == "slow");
        Assert.Contains(during.Events, e => e.SequencePosition == first.SequencePositions[0]);

        var warning = opened.ReadLogEntries().Single(e => e.Level == LogLevel.Warning && e.Message.Contains("Read horizon advanced", StringComparison.Ordinal));
        Assert.Matches(new Regex(@"held for \d+ms by session \d+ \(transaction \d+\)", RegexOptions.CultureInvariant), warning.Message);
        var held = long.Parse(Regex.Match(warning.Message, @"held for (\d+)ms").Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
        Assert.True(held > 250, warning.Message);

        release.TrySetResult();
        var slowResult = await slow.WaitAsync(TimeSpan.FromSeconds(15));
        var sawLate = await WaitForLogAsync(
            opened,
            e => e.Level == LogLevel.Error && e.Message.Contains("landed below a read horizon", StringComparison.Ordinal),
            TimeSpan.FromSeconds(5));
        Assert.Contains(slowResult.SequencePositions[0].ToString(System.Globalization.CultureInfo.InvariantCulture), sawLate, StringComparison.Ordinal);
        Assert.Contains("slow", sawLate, StringComparison.Ordinal);
    }

    [ContractFact]
    public Task HeldAppend_ShorterThanGrace_IsReadInOrder()
        => RunHeldWithinGrace(waitForever: false);

    [ContractFact]
    public Task HeldAppend_WaitForever_IsReadInOrder()
        => RunHeldWithinGrace(waitForever: true);

    private async Task RunHeldWithinGrace(bool waitForever)
    {
        SkipUnless(
            Cap.HasReadHorizon && Cap.CanPauseBeforeCommit,
            "In-memory appends commit in the same call and have no read horizon.");

        var grace = waitForever ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(10);
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(readHorizonGracePeriod: grace));
        var first = await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var slow = opened.Store.AppendAsync("slow", [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        var fast = await opened.Store.AppendAsync("fast", [ContractEvent.Tick()]);
        await Task.Delay(400);
        Assert.Equal(first.SequencePositions[0], await opened.Store.GetCurrentSequenceAsync());
        Assert.DoesNotContain(
            opened.ReadLogEntries(),
            e => e.Level == LogLevel.Warning && e.Message.Contains("Read horizon advanced", StringComparison.Ordinal));

        release.TrySetResult();
        var slowResult = await slow.WaitAsync(TimeSpan.FromSeconds(15));
        var read = await opened.Log.ReadByQueryAsync(Query.All());
        Assert.Equal(
            new[] { first.SequencePositions[0], slowResult.SequencePositions[0], fast.SequencePositions[0] },
            read.Events.Select(e => e.SequencePosition));
        Assert.DoesNotContain(
            opened.ReadLogEntries(),
            e => e.Level == LogLevel.Error && e.Message.Contains("landed below a read horizon", StringComparison.Ordinal));
    }

    [ContractFact]
    public async Task LostCommitReply_ReturnsTheLandedAppend_AndDoesNotRetry()
    {
        SkipUnless(Cap.CanDropCommitReply, "In-memory append does not have a commit reply that can be dropped.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        opened.Hooks.DropNextCommitReply();
        var result = await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        Assert.Equal(1, opened.Hooks.CommitAttempts);
        Assert.Equal(1, result.SequencePositions[0]);
        Assert.Single(await opened.Store.ReadStreamAsync("s1"));
    }

    [ContractFact]
    public async Task CommitAttemptedThenFailed_IsNotRetried()
    {
        SkipUnless(Cap.CanFailCommit, "In-memory append does not attempt a separate commit that can fail.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        await opened.Store.AppendAsync("s1", [ContractEvent.Tick()]);
        opened.Hooks.FailNextCommit();
        await Assert.ThrowsAsync<InvalidOperationException>(() => opened.Store.AppendAsync("s2", [ContractEvent.Tick()]));
        Assert.Equal(1, opened.Hooks.CommitAttempts);
        Assert.Empty(await opened.Store.ReadStreamAsync("s2"));
    }

    [ContractFact]
    public async Task PausedAppend_KillConnection_NextAppendStillCommits()
    {
        SkipUnless(
            Cap.CanPauseBeforeCommit && Cap.CanKillConnection,
            "In-memory has no connection to kill.");
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        opened.Hooks.PauseNextAppendBeforeCommit(entered, release);
        var slow = opened.Store.AppendAsync("slow", [ContractEvent.Tick()]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await opened.Hooks.KillOpenConnectionAsync();
        release.TrySetResult();
        await Assert.ThrowsAnyAsync<Exception>(() => slow.WaitAsync(TimeSpan.FromSeconds(15)));
        await opened.Store.AppendAsync("next", [ContractEvent.Tick()]);
        Assert.Single(await opened.Store.ReadStreamAsync("next"));
    }

    private static async Task<long> WaitForHeadAsync(IEventStore store, long expected, TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        long head = 0;
        while (DateTime.UtcNow - started < timeout)
        {
            head = await store.GetCurrentSequenceAsync();
            if (head == expected)
                return head;
            await Task.Delay(40);
        }

        return head;
    }

    private static async Task<string> WaitForLogAsync(
        ContractEventStore opened,
        Func<(LogLevel Level, string Message), bool> match,
        TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        while (DateTime.UtcNow - started < timeout)
        {
            await opened.Store.GetCurrentSequenceAsync();
            var found = opened.ReadLogEntries().FirstOrDefault(match);
            if (found.Message is not null)
                return found.Message;
            await Task.Delay(40);
        }

        return "";
    }
}
