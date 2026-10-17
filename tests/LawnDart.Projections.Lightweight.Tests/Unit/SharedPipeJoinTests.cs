using LawnDart.EventSourcing.EventStore;
using LawnDart.Projections.Lightweight.Hosting;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class SharedPipeJoinTests
{
    [Fact]
    public void TryAttach_UnknownSlot_DoesNotAttach()
    {
        var pipe = new LightweightProjectionSharedPipe(
            new InMemoryEventStore(),
            new LightweightProjectionOptions
            {
                UseSharedSubscribe = true,
                UseSubscribe = true
            },
            "default");

        Assert.False(pipe.TryAttachIfCaughtUp("missing", 3));
    }
}
