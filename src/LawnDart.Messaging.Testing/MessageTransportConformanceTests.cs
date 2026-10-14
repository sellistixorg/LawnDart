namespace LawnDart.Messaging.Testing;

/// <summary>
/// Proves a transport delivers, retries, dead-letters, and cancels,
/// and that user, tenant, and custom headers arrive unchanged.
/// A transport test project inherits this class and implements <see cref="CreateFixture"/>.
/// </summary>
public abstract class MessageTransportConformanceTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Builds a transport for one test.
    /// </summary>
    /// <param name="configure">Changes the settings before the transport starts. May be <c>null</c>.</param>
    /// <param name="preHandler">Hook registered for this fixture. May be <c>null</c>.</param>
    protected abstract TransportConformanceFixture CreateFixture(
        Action<MessageTransportOptions>? configure = null,
        IMessagePreHandler? preHandler = null);

    /// <summary>A published message reaches the subscriber.</summary>
    [Fact]
    public async Task Publish_delivers_the_message()
    {
        await using var fixture = CreateFixture();
        ConformancePing? received = null;
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await fixture.Transport.SubscribeAsync<ConformancePing>((message, _, _) =>
        {
            received = message;
            delivered.TrySetResult();
            return Task.CompletedTask;
        });

        var sent = new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "hello");
        await fixture.Transport.PublishAsync(sent, MessageContext.New());
        await delivered.Task.WaitAsync(Timeout);

        Assert.NotNull(received);
        Assert.Equal(sent.Text, received.Text);
        Assert.Equal(sent.Id, received.Id);
    }

    /// <summary>User, tenant, and custom headers the publisher set are the values the handler sees.</summary>
    [Fact]
    public async Task User_tenant_and_custom_headers_pass_through_unchanged()
    {
        await using var fixture = CreateFixture();
        MessageContext? received = null;
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var enqueuedAt = new DateTimeOffset(2026, 4, 1, 12, 30, 0, TimeSpan.Zero);

        await fixture.Transport.SubscribeAsync<ConformancePing>((_, context, _) =>
        {
            received = context;
            delivered.TrySetResult();
            return Task.CompletedTask;
        });

        var sent = new MessageContext
        {
            MessageId = "msg-7",
            CorrelationId = "corr-7",
            CausationId = "cause-7",
            UserId = "user-7",
            TenantId = "tenant-7",
            EnqueuedAt = enqueuedAt,
            Headers = new Dictionary<string, string>
            {
                ["region"] = "east",
                ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            },
        };

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "headers"),
            sent);
        await delivered.Task.WaitAsync(Timeout);

        Assert.NotNull(received);
        Assert.Equal("msg-7", received.MessageId);
        Assert.Equal("corr-7", received.CorrelationId);
        Assert.Equal("cause-7", received.CausationId);
        Assert.Equal("user-7", received.UserId);
        Assert.Equal("tenant-7", received.TenantId);
        Assert.Equal(enqueuedAt, received.EnqueuedAt);
        Assert.Equal("east", received.Headers["region"]);
        Assert.Equal(sent.Headers["traceparent"], received.Headers["traceparent"]);
    }

    /// <summary>The pre-handler hook runs, and it runs before the message handler.</summary>
    [Fact]
    public async Task Pre_handler_runs_before_the_message_handler()
    {
        var order = new List<string>();
        var gate = new object();
        var hook = new RecordingPreHandler(context =>
        {
            lock (gate)
                order.Add("pre:" + context.UserId);
            return Task.CompletedTask;
        });

        await using var fixture = CreateFixture(preHandler: hook);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await fixture.Transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            lock (gate)
                order.Add("handler");
            delivered.TrySetResult();
            return Task.CompletedTask;
        });

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "hook"),
            new MessageContext { MessageId = "msg-hook", UserId = "user-hook" });
        await delivered.Task.WaitAsync(Timeout);

        Assert.Equal(["pre:user-hook", "handler"], order);
    }

    /// <summary>A handler that fails and then succeeds runs once per attempt, and is not dead-lettered.</summary>
    [Fact]
    public async Task Handler_is_retried_until_it_succeeds_within_max_attempts()
    {
        await using var fixture = CreateFixture(options => options.MaxDeliveryAttempts = 3);
        var attempts = 0;
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await fixture.Transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            var attempt = Interlocked.Increment(ref attempts);
            if (attempt < 3)
                throw new InvalidOperationException("not yet");
            delivered.TrySetResult();
            return Task.CompletedTask;
        });

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "retry"),
            MessageContext.New());
        await delivered.Task.WaitAsync(Timeout);

        Assert.Equal(3, Volatile.Read(ref attempts));
        Assert.Empty(fixture.DeadLetters);
    }

    /// <summary>After the last allowed attempt, the message is dead-lettered and the handler is not called again.</summary>
    [Fact]
    public async Task Message_is_dead_lettered_after_max_attempts()
    {
        await using var fixture = CreateFixture(options =>
        {
            options.MaxDeliveryAttempts = 3;
            options.DeadLetterFailedMessages = true;
        });
        var attempts = 0;

        await fixture.Transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("nope");
        });

        var context = new MessageContext { MessageId = "msg-dead" };
        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "dead"),
            context);

        await WaitUntilAsync(() => fixture.DeadLetters.Count >= 1, Timeout);

        var parked = Assert.Single(fixture.DeadLetters);
        Assert.Equal(3, parked.Attempts);
        Assert.Equal(3, Volatile.Read(ref attempts));
        Assert.Equal("msg-dead", parked.Context.MessageId);
        Assert.Equal("messaging.conformance-ping", parked.MessageType);
        Assert.Contains("nope", parked.Error, StringComparison.Ordinal);
    }

    /// <summary>Cancelling the subscription stops later publishes from reaching that handler.</summary>
    [Fact]
    public async Task Cancelling_the_subscription_stops_delivery()
    {
        await using var fixture = CreateFixture();
        var calls = 0;
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource();

        var subscription = fixture.Transport.SubscribeAsync<ConformancePing>((_, _, _) =>
        {
            Interlocked.Increment(ref calls);
            delivered.TrySetResult();
            return Task.CompletedTask;
        }, cts.Token);

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "first"),
            MessageContext.New());
        await delivered.Task.WaitAsync(Timeout);
        Assert.Equal(1, Volatile.Read(ref calls));

        await cts.CancelAsync();
        await subscription.WaitAsync(Timeout);

        await fixture.Transport.PublishAsync(
            new ConformancePing(Guid.NewGuid(), DateTime.UtcNow, "second"),
            MessageContext.New());
        await Task.Delay(50);

        Assert.Equal(1, Volatile.Read(ref calls));
    }

    /// <summary>A message type without <c>[EventTypeName]</c> is rejected. The class name is not used.</summary>
    [Fact]
    public async Task Undeclared_message_type_is_rejected()
    {
        await using var fixture = CreateFixture();
        var publish = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Transport.PublishAsync(new UndeclaredPing(Guid.NewGuid(), DateTime.UtcNow), MessageContext.New()));
        Assert.Contains("EventTypeName", publish.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(UndeclaredPing).FullName!, publish.Message, StringComparison.Ordinal);

        var subscribe = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Transport.SubscribeAsync<UndeclaredPing>((_, _, _) => Task.CompletedTask));
        Assert.Contains("EventTypeName", subscribe.Message, StringComparison.Ordinal);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met before the timeout.");
            await Task.Delay(20).ConfigureAwait(false);
        }
    }

    private sealed class RecordingPreHandler : IMessagePreHandler
    {
        private readonly Func<MessageContext, Task> _before;

        public RecordingPreHandler(Func<MessageContext, Task> before) => _before = before;

        public Task BeforeHandleAsync(MessageContext context, CancellationToken cancellationToken)
            => _before(context);
    }

    private sealed record UndeclaredPing(Guid Id, DateTime Timestamp) : IEvent;
}
