using LawnDart.EventSourcing.Outbox;
using LawnDart.EventStore;
using LawnDart.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.EventSourcing.Tests.Outbox;

/// <summary>
/// With more than one publisher, a retry calls only the publishers that have not published the row.
/// </summary>
public class OutboxPublisherRetryTests
{
    [Fact]
    public async Task Retry_after_the_second_publisher_fails_does_not_call_the_first_again()
    {
        var writer = new InMemoryOutboxWriter(TimeSpan.Zero);
        var first = new CountingPublisher();
        var second = new CountingPublisher { FailuresLeft = 2 };
        var message = Message(1);
        await writer.WriteAsync(message);
        var processor = Processor(writer, first, second);

        await processor.ProcessBatchAsync(CancellationToken.None);
        var row = Assert.Single(writer.Snapshot());
        Assert.Equal(0b01, row.PublishedTo);
        Assert.Equal(1, row.Attempts);
        Assert.Null(row.ProcessedAt);

        await processor.ProcessBatchAsync(CancellationToken.None);
        await processor.ProcessBatchAsync(CancellationToken.None);

        Assert.Equal(1, first.Calls);
        Assert.Equal(3, second.Calls);
        Assert.Equal(1, second.Successes);
        row = Assert.Single(writer.Snapshot());
        Assert.NotNull(row.ProcessedAt);
        Assert.Equal(2, row.Attempts);
    }

    [Fact]
    public async Task Publishers_after_the_failed_one_run_on_the_retry()
    {
        var writer = new InMemoryOutboxWriter(TimeSpan.Zero);
        var first = new CountingPublisher();
        var second = new CountingPublisher { FailuresLeft = 1 };
        var third = new CountingPublisher();
        await writer.WriteAsync(Message(1));
        var processor = Processor(writer, first, second, third);

        await processor.ProcessBatchAsync(CancellationToken.None);
        Assert.Equal(0, third.Calls);

        await processor.ProcessBatchAsync(CancellationToken.None);

        Assert.Equal(1, first.Successes);
        Assert.Equal(1, second.Successes);
        Assert.Equal(1, third.Successes);
        Assert.Equal(1, first.Calls);
        Assert.NotNull(Assert.Single(writer.Snapshot()).ProcessedAt);
    }

    [Fact]
    public async Task Row_that_already_records_a_publisher_skips_it()
    {
        var writer = new InMemoryOutboxWriter(TimeSpan.Zero);
        var first = new CountingPublisher();
        var second = new CountingPublisher();
        var message = Message(1);
        message.PublishedTo = 0b01;
        await writer.WriteAsync(message);

        await Processor(writer, first, second).ProcessBatchAsync(CancellationToken.None);

        Assert.Equal(0, first.Calls);
        Assert.Equal(1, second.Calls);
        Assert.NotNull(Assert.Single(writer.Snapshot()).ProcessedAt);
    }

    [Fact]
    public async Task Failed_mark_as_processed_does_not_publish_again()
    {
        var inner = new InMemoryOutboxWriter(TimeSpan.Zero);
        var writer = new FailMarkOnceWriter(inner);
        var first = new CountingPublisher();
        var second = new CountingPublisher();
        await writer.WriteAsync(Message(1));
        var processor = Processor(writer, first, second);

        await processor.ProcessBatchAsync(CancellationToken.None);
        var row = Assert.Single(inner.Snapshot());
        Assert.Null(row.ProcessedAt);
        Assert.Equal(0b11, row.PublishedTo);

        await processor.ProcessBatchAsync(CancellationToken.None);

        Assert.Equal(1, first.Calls);
        Assert.Equal(1, second.Calls);
        Assert.NotNull(Assert.Single(inner.Snapshot()).ProcessedAt);
    }

    [Fact]
    public async Task Single_publisher_failure_records_no_publishers()
    {
        var writer = new InMemoryOutboxWriter(TimeSpan.Zero);
        var only = new CountingPublisher { FailuresLeft = 1 };
        await writer.WriteAsync(Message(1));
        var processor = Processor(writer, only);

        await processor.ProcessBatchAsync(CancellationToken.None);
        Assert.Equal(0, Assert.Single(writer.Snapshot()).PublishedTo);

        await processor.ProcessBatchAsync(CancellationToken.None);
        Assert.Equal(2, only.Calls);
        Assert.NotNull(Assert.Single(writer.Snapshot()).ProcessedAt);
    }

    [Fact]
    public async Task Sixty_four_publishers_use_every_bit()
    {
        var writer = new InMemoryOutboxWriter(TimeSpan.Zero);
        var publishers = Enumerable.Range(0, OutboxProcessor.MaxPublishers)
            .Select(_ => new CountingPublisher())
            .ToArray();
        publishers[^1].FailuresLeft = 1;
        await writer.WriteAsync(Message(1));
        var processor = Processor(writer, publishers);

        await processor.ProcessBatchAsync(CancellationToken.None);
        Assert.Equal(long.MaxValue, Assert.Single(writer.Snapshot()).PublishedTo);

        await processor.ProcessBatchAsync(CancellationToken.None);

        Assert.All(publishers, publisher => Assert.Equal(1, publisher.Successes));
        Assert.All(publishers[..^1], publisher => Assert.Equal(1, publisher.Calls));
        Assert.NotNull(Assert.Single(writer.Snapshot()).ProcessedAt);
    }

    [Fact]
    public void More_than_sixty_four_publishers_are_rejected()
    {
        var publishers = Enumerable.Range(0, OutboxProcessor.MaxPublishers + 1)
            .Select(_ => (IOutboxPublisher)new CountingPublisher())
            .ToArray();

        var ex = Assert.Throws<ArgumentException>(() => new OutboxProcessor(
            new InMemoryOutboxWriter(),
            publishers,
            NullLogger<OutboxProcessor>.Instance,
            lifetime: null));
        Assert.Contains("at most 64", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Host_registration_rejects_more_than_sixty_four_publishers()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.Configure<LawnDart.LawnDartOptions>(options => options.RequireTenantId = false);
        for (var i = 0; i <= OutboxProcessor.MaxPublishers; i++)
            services.AddSingleton<IOutboxPublisher>(new CountingPublisher());
        services.AddBoundedContext("default").WithEventTypes(typeof(RetryPing))
            .PublishEvents(events => events.Publish<RetryPing>())
            .UseInMemory(options => options.EnableOutbox = true);
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetServices<IHostedService>().ToArray());
        Assert.Contains("at most 64", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Host_with_two_registered_publishers_sends_each_row_once_to_each()
    {
        var first = new CountingPublisher();
        var second = new CountingPublisher { FailuresLeft = 1 };
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<LawnDart.LawnDartOptions>(options => options.RequireTenantId = false);
        builder.Services.AddSingleton<IOutboxPublisher>(first);
        builder.Services.AddSingleton<IOutboxPublisher>(second);
        builder.Services.AddBoundedContext("default")
            .WithEventTypes(typeof(RetryPing))
            .PublishEvents(events => events.Publish<RetryPing>())
            .UseInMemory(options =>
            {
                options.EnableOutbox = true;
                options.OutboxClaimLease = TimeSpan.Zero;
            });
        using var host = builder.Build();
        var writer = (InMemoryOutboxWriter)host.Services.GetRequiredKeyedService<IOutboxWriter>("default");
        await writer.WriteAsync(Message(1));

        await host.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
        try
        {
            await WaitUntilAsync(() => writer.Snapshot().Single().ProcessedAt is not null, TimeSpan.FromSeconds(20));
        }
        finally
        {
            await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.Equal(1, first.Calls);
        Assert.Equal(2, second.Calls);
        Assert.Equal(1, second.Successes);
    }

    private static TestProcessor Processor(IOutboxWriter writer, params IOutboxPublisher[] publishers)
        => new(writer, publishers);

    private static OutboxMessage Message(long sequence)
        => new()
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = sequence
        };

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!condition())
            await Task.Delay(20, cts.Token);
    }

    private sealed class TestProcessor : OutboxProcessor
    {
        public TestProcessor(IOutboxWriter writer, IReadOnlyList<IOutboxPublisher> publishers)
            : base(writer, publishers, NullLogger<OutboxProcessor>.Instance, lifetime: null, TimeSpan.FromSeconds(5), batchSize: 100, maxAttempts: 10)
        {
        }

        public new Task ProcessBatchAsync(CancellationToken cancellationToken)
            => base.ProcessBatchAsync(cancellationToken);
    }

    private sealed class CountingPublisher : IOutboxPublisher
    {
        private int _calls;
        private int _successes;

        public int FailuresLeft { get; set; }

        public int Calls => Volatile.Read(ref _calls);

        public int Successes => Volatile.Read(ref _successes);

        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _calls);
            if (FailuresLeft > 0)
            {
                FailuresLeft--;
                throw new InvalidOperationException("Simulated publish failure");
            }

            Interlocked.Increment(ref _successes);
            return Task.CompletedTask;
        }
    }

    private sealed class FailMarkOnceWriter : IOutboxWriter
    {
        private readonly InMemoryOutboxWriter _inner;
        private bool _failed;

        public FailMarkOnceWriter(InMemoryOutboxWriter inner) => _inner = inner;

        public Task WriteAsync(OutboxMessage message, CancellationToken cancellationToken = default)
            => _inner.WriteAsync(message, cancellationToken);

        public Task WriteBatchAsync(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken = default)
            => _inner.WriteBatchAsync(messages, cancellationToken);

        public Task<IReadOnlyList<OutboxMessage>> GetUnprocessedAsync(int batchSize, CancellationToken cancellationToken = default)
            => _inner.GetUnprocessedAsync(batchSize, cancellationToken);

        public Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
        {
            if (!_failed)
            {
                _failed = true;
                throw new InvalidOperationException("database unavailable");
            }

            return _inner.MarkAsProcessedAsync(messageId, cancellationToken);
        }

        public Task RecordFailureAsync(
            Guid messageId, string error, long publishedTo = 0, CancellationToken cancellationToken = default)
            => _inner.RecordFailureAsync(messageId, error, publishedTo, cancellationToken);

        public Task MarkAsDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
            => _inner.MarkAsDeadLetteredAsync(messageId, cancellationToken);

        public Task<IReadOnlyList<OutboxMessage>> GetDeadLetteredAsync(int batchSize, CancellationToken cancellationToken = default)
            => _inner.GetDeadLetteredAsync(batchSize, cancellationToken);

        public Task<bool> ResetDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
            => _inner.ResetDeadLetteredAsync(messageId, cancellationToken);

        public Task<int> ResetAllDeadLetteredAsync(CancellationToken cancellationToken = default)
            => _inner.ResetAllDeadLetteredAsync(cancellationToken);

        public Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
            => _inner.InitializeSchemaAsync(cancellationToken);
    }

    [EventTypeName("outbox-retry.ping")]
    private sealed record RetryPing(Guid Id, DateTime Timestamp) : IEvent;
}
