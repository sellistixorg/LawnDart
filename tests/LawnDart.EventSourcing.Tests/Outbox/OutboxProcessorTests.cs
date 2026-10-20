using System.Diagnostics;
using Xunit;
using LawnDart;
using LawnDart.Outbox;
using LawnDart.EventSourcing.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.TestUtilities;

namespace LawnDart.EventSourcing.Tests.Outbox;

public class OutboxProcessorTests
{
    [Fact]
    public async Task ProcessBatch_WithUnprocessedMessages_ProcessesThem()
    {
        // Arrange
        var writer = new InMemoryOutboxWriter();
        var publisher = new TestOutboxPublisher();
        
        await writer.WriteAsync(new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        });

        var processor = new TestableOutboxProcessor(writer, publisher);

        // Act
        await processor.ProcessBatchPublicAsync(CancellationToken.None);

        // Assert
        Assert.Single(publisher.PublishedMessages);
        var unprocessed = await writer.GetUnprocessedAsync(10);
        Assert.Empty(unprocessed);
    }

    [Fact]
    public async Task GetUnprocessed_ClaimsTheRow_UntilFailureReleasesIt()
    {
        var writer = new InMemoryOutboxWriter();
        var id = Guid.NewGuid();
        await writer.WriteAsync(new OutboxMessage
        {
            Id = id,
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        });

        Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Empty(await writer.GetUnprocessedAsync(10));
        await writer.RecordFailureAsync(id, "boom");
        Assert.Equal(id, Assert.Single(await writer.GetUnprocessedAsync(10)).Id);
    }

    [Fact]
    public async Task ProcessBatch_WithPublishFailure_RecordsFailure()
    {
        // Arrange
        var writer = new InMemoryOutboxWriter();
        var publisher = new TestOutboxPublisher(shouldFail: true);
        
        var messageId = Guid.NewGuid();
        await writer.WriteAsync(new OutboxMessage
        {
            Id = messageId,
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        });

        var processor = new TestableOutboxProcessor(writer, publisher);

        // Act
        await processor.ProcessBatchPublicAsync(CancellationToken.None);

        // Assert
        var unprocessed = await writer.GetUnprocessedAsync(10);
        Assert.Single(unprocessed);
        Assert.Equal(1, unprocessed[0].Attempts);
        Assert.NotNull(unprocessed[0].LastError);
    }

    [Fact]
    public async Task ProcessBatch_WithMaxAttemptsExceeded_DeadLettersMessage()
    {
        var writer = new InMemoryOutboxWriter();
        var publisher = new TestOutboxPublisher();

        var messageId = Guid.NewGuid();
        await writer.WriteAsync(new OutboxMessage
        {
            Id = messageId,
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1,
            Attempts = 10
        });

        var processor = new TestableOutboxProcessor(writer, publisher, maxAttempts: 10);

        await processor.ProcessBatchPublicAsync(CancellationToken.None);

        Assert.Empty(publisher.PublishedMessages);
        Assert.Empty(await writer.GetUnprocessedAsync(10));
        var dead = await writer.GetDeadLetteredAsync(10);
        Assert.Single(dead);
        Assert.Equal(messageId, dead[0].Id);
        Assert.NotNull(dead[0].DeadLetteredAt);
        Assert.Null(dead[0].ProcessedAt);
    }

    [Fact]
    public async Task ProcessBatch_DeadLetterDoesNotBlockLaterMessages()
    {
        var writer = new InMemoryOutboxWriter();
        var publisher = new TestOutboxPublisher();

        var poisonId = Guid.NewGuid();
        var laterId = Guid.NewGuid();
        await writer.WriteAsync(new OutboxMessage
        {
            Id = poisonId,
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1,
            Attempts = 10
        });
        await writer.WriteAsync(new OutboxMessage
        {
            Id = laterId,
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 2
        });

        var processor = new TestableOutboxProcessor(writer, publisher, maxAttempts: 10);

        await processor.ProcessBatchPublicAsync(CancellationToken.None);

        Assert.Single(publisher.PublishedMessages);
        Assert.Equal(laterId, publisher.PublishedMessages[0].Id);
        Assert.Empty(await writer.GetUnprocessedAsync(10));
        var dead = await writer.GetDeadLetteredAsync(10);
        Assert.Single(dead);
        Assert.Equal(poisonId, dead[0].Id);
    }

    [Fact]
    public async Task ProcessBatch_FailureReachingMaxAttempts_DeadLettersImmediately()
    {
        var writer = new InMemoryOutboxWriter();
        var publisher = new TestOutboxPublisher(shouldFail: true);

        var messageId = Guid.NewGuid();
        await writer.WriteAsync(new OutboxMessage
        {
            Id = messageId,
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1,
            Attempts = 9
        });

        var processor = new TestableOutboxProcessor(writer, publisher, maxAttempts: 10);

        await processor.ProcessBatchPublicAsync(CancellationToken.None);

        Assert.Empty(publisher.PublishedMessages);
        Assert.Empty(await writer.GetUnprocessedAsync(10));
        var dead = await writer.GetDeadLetteredAsync(10);
        Assert.Single(dead);
        Assert.Equal(messageId, dead[0].Id);
        Assert.Equal(10, dead[0].Attempts);
        Assert.NotNull(dead[0].LastError);
    }

    [Fact]
    public async Task ProcessBatch_AfterDeadLetterReset_PublishesAndMarksProcessed()
    {
        var writer = new InMemoryOutboxWriter();
        var publisher = new TestOutboxPublisher(shouldFail: true);
        var messageId = Guid.NewGuid();
        await writer.WriteAsync(new OutboxMessage
        {
            Id = messageId,
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        });

        var processor = new TestableOutboxProcessor(writer, publisher, maxAttempts: 3);
        for (var i = 0; i < 5; i++)
        {
            await processor.ProcessBatchPublicAsync(CancellationToken.None);
            if ((await writer.GetDeadLetteredAsync(10)).Count == 1)
                break;
        }

        var dead = Assert.Single(await writer.GetDeadLetteredAsync(10));
        Assert.Equal(messageId, dead.Id);
        Assert.Null(dead.ProcessedAt);

        Assert.True(await writer.ResetDeadLetteredAsync(messageId));

        publisher.ShouldFail = false;
        await processor.ProcessBatchPublicAsync(CancellationToken.None);

        Assert.Contains(publisher.PublishedMessages, m => m.Id == messageId);
        Assert.Empty(await writer.GetUnprocessedAsync(10));
        Assert.Empty(await writer.GetDeadLetteredAsync(10));
    }

    [Fact]
    public async Task ProcessBatch_ProcessesMultipleMessages()
    {
        // Arrange
        var writer = new InMemoryOutboxWriter();
        var publisher = new TestOutboxPublisher();
        
        for (int i = 0; i < 3; i++)
        {
            await writer.WriteAsync(new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = "TestEvent",
                Payload = System.Text.Encoding.UTF8.GetBytes($"{{\"id\":{i}}}"),
                Metadata = "{}",
                CreatedAt = DateTime.UtcNow,
                StreamId = "stream1",
                SequencePosition = i
            });
        }

        var processor = new TestableOutboxProcessor(writer, publisher, batchSize: 100);

        // Act
        await processor.ProcessBatchPublicAsync(CancellationToken.None);

        // Assert
        Assert.Equal(3, publisher.PublishedMessages.Count);
        var unprocessed = await writer.GetUnprocessedAsync(10);
        Assert.Empty(unprocessed);
    }

    [Fact]
    public async Task ProcessBatch_AgainstUseInMemoryWriter_ProcessesThem()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMetadataProvider>(new DefaultMetadataProvider());
        services.AddSingleton<ITenantContextProvider>(new TestTenantContextProvider(null));
        services.Configure<LawnDartOptions>(_ => { });
        services.AddBoundedContext("default").UseInMemory().WithEventTypes(typeof(CatalogPlaceholderEvent));
        var sp = services.BuildServiceProvider();

        var writer = sp.GetRequiredService<IOutboxWriter>();
        var publisher = new TestOutboxPublisher();
        await writer.WriteAsync(new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = 1
        });

        var processor = new TestableOutboxProcessor(writer, publisher);
        await processor.ProcessBatchPublicAsync(CancellationToken.None);

        Assert.Single(publisher.PublishedMessages);
        Assert.Empty(await writer.GetUnprocessedAsync(10));
    }

    [Fact]
    public async Task Processor_WakesOnSignal_BeforeThePollInterval()
    {
        var writer = new InMemoryOutboxWriter();
        var publisher = new GatePublisher();
        var signal = new NewEventsSignal();
        var processor = new OutboxProcessor(
            writer,
            publisher,
            NullLogger<OutboxProcessor>.Instance,
            pollingInterval: TimeSpan.FromSeconds(30),
            newEvents: signal);

        await processor.StartAsync(CancellationToken.None);
        try
        {
            await WaitForWaitersAsync(signal, 1);
            await writer.WriteAsync(SampleMessage());

            var started = Stopwatch.StartNew();
            signal.Notify(1);
            await publisher.Published.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.True(started.Elapsed < TimeSpan.FromSeconds(2), $"published after {started.Elapsed}");
        }
        finally
        {
            await processor.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Processor_PollsWhenTheSignalIsNeverFired()
    {
        var writer = new InMemoryOutboxWriter();
        var publisher = new GatePublisher();
        var signal = new NewEventsSignal();
        var processor = new OutboxProcessor(
            writer,
            publisher,
            NullLogger<OutboxProcessor>.Instance,
            pollingInterval: TimeSpan.FromMilliseconds(50),
            newEvents: signal);

        await processor.StartAsync(CancellationToken.None);
        try
        {
            await WaitForWaitersAsync(signal, 1);
            await writer.WriteAsync(SampleMessage());
            await publisher.Published.WaitAsync(TimeSpan.FromSeconds(3));
            Assert.Equal(0, signal.Generation);
        }
        finally
        {
            await processor.StopAsync(CancellationToken.None);
        }
    }

    private static OutboxMessage SampleMessage() => new()
    {
        Id = Guid.NewGuid(),
        EventType = "TestEvent",
        Payload = "{}"u8.ToArray(),
        Metadata = "{}",
        CreatedAt = DateTime.UtcNow,
        StreamId = "stream1",
        SequencePosition = 1
    };

    private static async Task WaitForWaitersAsync(NewEventsSignal signal, int count)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (signal.WaitingCount < count)
            await Task.Delay(10, cts.Token);
    }

    private sealed class GatePublisher : IOutboxPublisher
    {
        private readonly TaskCompletionSource _published = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Published => _published.Task;

        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            _published.TrySetResult();
            return Task.CompletedTask;
        }
    }
}

public class TestOutboxPublisher : IOutboxPublisher
{
    public bool ShouldFail { get; set; }
    public List<OutboxMessage> PublishedMessages { get; } = new();

    public TestOutboxPublisher(bool shouldFail = false)
    {
        ShouldFail = shouldFail;
    }

    public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        if (ShouldFail)
        {
            throw new InvalidOperationException("Simulated publish failure");
        }

        PublishedMessages.Add(message);
        return Task.CompletedTask;
    }
}

public class TestableOutboxProcessor : OutboxProcessor
{
    public TestableOutboxProcessor(
        IOutboxWriter outboxWriter,
        IOutboxPublisher outboxPublisher,
        int batchSize = 100,
        int maxAttempts = 10)
        : base(
            outboxWriter,
            outboxPublisher,
            NullLogger<OutboxProcessor>.Instance,
            TimeSpan.FromSeconds(5),
            batchSize,
            maxAttempts)
    {
    }

    public Task ProcessBatchPublicAsync(CancellationToken cancellationToken)
    {
        // Access the private ProcessBatchAsync method via reflection
        var method = typeof(OutboxProcessor).GetMethod(
            "ProcessBatchAsync",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        return (Task)method!.Invoke(this, new object[] { cancellationToken })!;
    }
}
