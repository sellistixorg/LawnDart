using Xunit;
using LawnDart.Outbox;
using LawnDart.EventSourcing.Outbox;
using Microsoft.Extensions.Logging.Abstractions;

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
            Payload = "{}",
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
            Payload = "{}",
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
            Payload = "{}",
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
            Payload = "{}",
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
            Payload = "{}",
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
            Payload = "{}",
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
                Payload = $"{{\"id\":{i}}}",
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
}

// Test helpers
public class InMemoryOutboxWriter : IOutboxWriter
{
    private readonly List<OutboxMessage> _messages = new();

    public Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    public Task WriteAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        _messages.Add(message);
        return Task.CompletedTask;
    }

    public Task WriteBatchAsync(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken = default)
    {
        _messages.AddRange(messages);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<OutboxMessage>> GetUnprocessedAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        var unprocessed = _messages
            .Where(m => m.ProcessedAt == null && m.DeadLetteredAt == null)
            .OrderBy(m => m.SequencePosition)
            .Take(batchSize)
            .ToList();
        return Task.FromResult<IReadOnlyList<OutboxMessage>>(unprocessed);
    }

    public Task<IReadOnlyList<OutboxMessage>> GetDeadLetteredAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        var dead = _messages
            .Where(m => m.DeadLetteredAt != null)
            .OrderBy(m => m.SequencePosition)
            .Take(batchSize)
            .ToList();
        return Task.FromResult<IReadOnlyList<OutboxMessage>>(dead);
    }

    public Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var message = _messages.FirstOrDefault(m => m.Id == messageId);
        if (message != null)
        {
            message.ProcessedAt = DateTime.UtcNow;
        }
        return Task.CompletedTask;
    }

    public Task MarkAsDeadLetteredAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var message = _messages.FirstOrDefault(m => m.Id == messageId);
        if (message != null && message.DeadLetteredAt is null)
        {
            message.DeadLetteredAt = DateTime.UtcNow;
        }
        return Task.CompletedTask;
    }

    public Task RecordFailureAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        var message = _messages.FirstOrDefault(m => m.Id == messageId);
        if (message != null)
        {
            message.Attempts++;
            message.LastError = error;
            message.LastAttemptAt = DateTime.UtcNow;
        }
        return Task.CompletedTask;
    }
}

public class TestOutboxPublisher : IOutboxPublisher
{
    private readonly bool _shouldFail;
    public List<OutboxMessage> PublishedMessages { get; } = new();

    public TestOutboxPublisher(bool shouldFail = false)
    {
        _shouldFail = shouldFail;
    }

    public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        if (_shouldFail)
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
