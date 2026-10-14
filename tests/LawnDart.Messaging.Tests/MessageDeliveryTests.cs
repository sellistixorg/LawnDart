using LawnDart.EventStore;

namespace LawnDart.Messaging.Tests;

public class MessageDeliveryTests
{
    [Fact]
    public async Task Pre_handler_runs_before_the_handler_on_each_attempt()
    {
        var options = new MessageTransportOptions
        {
            MaxDeliveryAttempts = 2,
            DeadLetterFailedMessages = true,
        };
        var order = new List<string>();
        var hook = new CountingPreHandler(() => order.Add("pre"));

        await MessageDelivery.DeliverAsync(
            new NamedPing(Guid.NewGuid(), DateTime.UtcNow),
            MessageContext.New(),
            (_, _, _) =>
            {
                order.Add("handler");
                throw new InvalidOperationException("again");
            },
            options,
            [hook],
            new MemoryDeadLetterQueue(),
            CancellationToken.None);

        Assert.Equal(["pre", "handler", "pre", "handler"], order);
    }

    [Fact]
    public async Task Deliver_throws_after_max_attempts_when_dead_letter_is_off()
    {
        var options = new MessageTransportOptions
        {
            MaxDeliveryAttempts = 2,
            DeadLetterFailedMessages = false,
        };
        var queue = new MemoryDeadLetterQueue();
        var attempts = 0;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            MessageDelivery.DeliverAsync(
                new NamedPing(Guid.NewGuid(), DateTime.UtcNow),
                MessageContext.New(),
                (_, _, _) =>
                {
                    attempts++;
                    throw new InvalidOperationException("boom");
                },
                options,
                [],
                queue,
                CancellationToken.None));

        Assert.Equal("boom", ex.Message);
        Assert.Equal(2, attempts);
        Assert.Empty(queue.ReadAll());
    }

    [Fact]
    public async Task Deliver_does_not_dead_letter_when_cancelled()
    {
        var options = new MessageTransportOptions { MaxDeliveryAttempts = 3 };
        var queue = new MemoryDeadLetterQueue();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            MessageDelivery.DeliverAsync(
                new NamedPing(Guid.NewGuid(), DateTime.UtcNow),
                MessageContext.New(),
                (_, _, _) => Task.CompletedTask,
                options,
                [],
                queue,
                cts.Token));

        Assert.Empty(queue.ReadAll());
    }

    [Fact]
    public void EnsureValid_rejects_bad_settings()
    {
        var options = new MessageTransportOptions { Concurrency = 0, Prefetch = -1, MaxDeliveryAttempts = 0, SubscriptionName = " " };
        var ex = Assert.Throws<InvalidOperationException>(options.EnsureValid);
        Assert.Contains("SubscriptionName", ex.Message, StringComparison.Ordinal);
    }

    [EventTypeName("messaging.named-ping")]
    private sealed record NamedPing(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class CountingPreHandler : IMessagePreHandler
    {
        private readonly Action _before;

        public CountingPreHandler(Action before) => _before = before;

        public Task BeforeHandleAsync(MessageContext context, CancellationToken cancellationToken)
        {
            _before();
            return Task.CompletedTask;
        }
    }
}
