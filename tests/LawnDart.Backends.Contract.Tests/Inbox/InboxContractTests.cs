using LawnDart;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Messaging.Hosting;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.Reaction;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.Backends.Contract.Tests;

public abstract class InboxContractTests : ContractSuite
{
    protected InboxContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task Unknown_id_is_not_processed()
    {
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated());
        Assert.False(await inbox.Store.IsProcessedAsync("never-seen"));
    }

    [Fact]
    public async Task Mark_then_lookup_is_true_and_a_second_mark_does_not_throw()
    {
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated());
        await inbox.Store.MarkProcessedAsync("msg-1", DateTimeOffset.UtcNow);
        Assert.True(await inbox.Store.IsProcessedAsync("msg-1"));
        var ex = await Record.ExceptionAsync(() => inbox.Store.MarkProcessedAsync("msg-1", DateTimeOffset.UtcNow));
        Assert.Null(ex);
        if (inbox.CountRowsAsync is not null)
            Assert.Equal(1, await inbox.CountRowsAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Expired_row_is_not_processed()
    {
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated(inboxDeduplicationWindow: TimeSpan.FromHours(1)));
        await inbox.Store.MarkProcessedAsync("fresh", DateTimeOffset.UtcNow);
        await inbox.Store.MarkProcessedAsync("stale", DateTimeOffset.UtcNow - TimeSpan.FromHours(2));
        Assert.True(await inbox.Store.IsProcessedAsync("fresh"));
        Assert.False(await inbox.Store.IsProcessedAsync("stale"));
    }

    [ContractFact]
    public async Task PurgeExpired_RemovesOnlyExpiredRows()
    {
        SkipUnless(Cap.CanPurgeExpiredInboxRows, "In-memory drops an expired mark on read and has no purge call.");
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated(inboxDeduplicationWindow: TimeSpan.FromHours(1)));
        await inbox.Store.MarkProcessedAsync("fresh", DateTimeOffset.UtcNow);
        await inbox.Store.MarkProcessedAsync("stale", DateTimeOffset.UtcNow - TimeSpan.FromHours(2));
        Assert.Equal(1, await inbox.PurgeExpiredAsync!(CancellationToken.None));
        Assert.True(await inbox.Store.IsProcessedAsync("fresh"));
        Assert.False(await inbox.Store.IsProcessedAsync("stale"));
        Assert.Equal(1, await inbox.CountRowsAsync!(CancellationToken.None));
    }

    [ContractFact]
    public async Task A_new_store_instance_sees_prior_marks()
    {
        SkipUnless(
            Cap.InboxMarksSurviveASecondInstance,
            "A second in-memory inbox does not see marks from the first.");
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated());
        await inbox.Store.MarkProcessedAsync("durable", DateTimeOffset.UtcNow);
        var second = await inbox.OpenSecondInstanceAsync!(CancellationToken.None);
        Assert.True(await second.IsProcessedAsync("durable"));
    }

    [Fact]
    public async Task Parallel_marks_of_one_id_leave_one_processed_id()
    {
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated());
        var processedAt = DateTimeOffset.UtcNow;
        var tasks = Enumerable.Range(0, 20).Select(_ => inbox.Store.MarkProcessedAsync("same-id", processedAt));
        var ex = await Record.ExceptionAsync(() => Task.WhenAll(tasks));
        Assert.Null(ex);
        Assert.True(await inbox.Store.IsProcessedAsync("same-id"));
        if (inbox.CountRowsAsync is not null)
            Assert.Equal(1, await inbox.CountRowsAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Reactor_drops_a_duplicate_delivery()
    {
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated());
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var reactor = new CountingReactor();
        var service = new ReactorHostedService<CountingReactor, PingEvent>(
            reactor,
            transport,
            inbox.Store,
            NullLogger<ReactorHostedService<CountingReactor, PingEvent>>.Instance);
        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await WaitForSubscriberAsync(transport);
        var context = new MessageContext { MessageId = "dup-1" };
        var evt = new PingEvent(Guid.NewGuid(), DateTime.UtcNow);
        await transport.PublishAsync(evt, context);
        await transport.PublishAsync(evt, context);
        Assert.Equal(1, reactor.CallCount);
        if (Cap.ExposesStoredInboxKey)
            Assert.Equal(1, await inbox.CountRowsAsync!(CancellationToken.None));
        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [ContractFact]
    public async Task Reactor_stores_the_scoped_consumer_key()
    {
        SkipUnless(
            Cap.ExposesStoredInboxKey,
            "In-memory does not expose the stored inbox key.");
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated());
        var transport = new InMemoryMessageTransport(NullLogger<InMemoryMessageTransport>.Instance);
        var reactor = new CountingReactor();
        var service = new ReactorHostedService<CountingReactor, PingEvent>(
            reactor,
            transport,
            inbox.Store,
            NullLogger<ReactorHostedService<CountingReactor, PingEvent>>.Instance);
        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await WaitForSubscriberAsync(transport);
        await transport.PublishAsync(new PingEvent(Guid.NewGuid(), DateTime.UtcNow), new MessageContext { MessageId = "dup-1" });
        await WaitUntilAsync(async () => await inbox.CountRowsAsync!(CancellationToken.None) == 1);
        var storedKey = await inbox.ReadSingleKeyAsync!(CancellationToken.None);
        Assert.Equal(InboxConsumerKey.ForReactor(typeof(CountingReactor), "dup-1"), storedKey);
        Assert.EndsWith(":dup-1", storedKey, StringComparison.Ordinal);
        Assert.NotEqual("dup-1", storedKey);
        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Claim_lease_override_is_honored()
    {
        var held = await Backend.OpenInboxAsync(ContractStores.Isolated(inboxClaimLease: TimeSpan.FromHours(1)));
        Assert.True(await held.Store.TryClaimAsync("held"));
        Assert.False(await held.Store.TryClaimAsync("held"));

        var open = await Backend.OpenInboxAsync(ContractStores.Isolated(inboxClaimLease: TimeSpan.Zero));
        Assert.True(await open.Store.TryClaimAsync("open"));
        Assert.True(await open.Store.TryClaimAsync("open"));
    }

    [Fact]
    public async Task TryClaim_SecondCallerLoses_UntilRelease()
    {
        var inbox = await Backend.OpenInboxAsync(ContractStores.Isolated(inboxClaimLease: TimeSpan.FromHours(1)));
        Assert.True(await inbox.Store.TryClaimAsync("key"));
        Assert.False(await inbox.Store.TryClaimAsync("key"));
        await inbox.Store.ReleaseClaimAsync("key");
        Assert.True(await inbox.Store.TryClaimAsync("key"));
        await inbox.Store.MarkProcessedAsync("key", DateTimeOffset.UtcNow);
        Assert.False(await inbox.Store.TryClaimAsync("key"));
    }

    private static async Task WaitForSubscriberAsync(InMemoryMessageTransport transport)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (transport.SubscriberCount<PingEvent>() < 1)
            await Task.Delay(20, cts.Token);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!await condition())
            await Task.Delay(20, cts.Token);
    }

    [EventTypeName("inbox-contract.ping")]
    private sealed record PingEvent(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class CountingReactor : IReactor<PingEvent>
    {
        public int CallCount { get; private set; }

        public Task<IEnumerable<ICommand>> ReactAsync(PingEvent @event, MessageContext context, CancellationToken cancellationToken = default)
        {
            CallCount++;
            return Task.FromResult<IEnumerable<ICommand>>([]);
        }
    }
}
