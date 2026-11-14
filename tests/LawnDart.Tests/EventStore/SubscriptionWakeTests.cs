using LawnDart.EventStore;

namespace LawnDart.Tests.EventStore;

public class SubscriptionWakeTests
{
    private static readonly TimeSpan Idle = TimeSpan.FromSeconds(30);

    [Fact]
    public void HiddenCommittedPosition_RetriesWithoutTheIdlePoll()
    {
        var wait = SubscriptionWake.IdleWait(Idle, visibleHead: 4, signaledPosition: 5, horizonHeld: true);

        Assert.Equal(SubscriptionWake.HiddenPositionRetry, wait);
        Assert.True(wait < Idle);
    }

    [Fact]
    public void QuietHead_KeepsTheIdlePoll()
    {
        Assert.Equal(Idle, SubscriptionWake.IdleWait(Idle, visibleHead: 5, signaledPosition: 5, horizonHeld: false));
        Assert.Equal(Idle, SubscriptionWake.IdleWait(Idle, visibleHead: 5, signaledPosition: 8, horizonHeld: false));
        Assert.Equal(Idle, SubscriptionWake.IdleWait(Idle, visibleHead: 5, signaledPosition: 5, horizonHeld: true));
        Assert.Equal(Idle, SubscriptionWake.IdleWait(Idle, visibleHead: 6, signaledPosition: 5, horizonHeld: true));
    }
}
