using LawnDart.EventSourcing.Outbox;
using LawnDart.Outbox;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.EventSourcing.Tests.Outbox;

public class OutboxShutdownTests
{
    [Fact]
    public async Task Startup_wait_ends_when_the_host_never_starts()
    {
        var writer = new InMemoryOutboxWriter();
        var publisher = new RecordingPublisher();
        var lifetime = new LifetimeStub(started: false);
        var processor = new OutboxProcessor(
            writer,
            publisher,
            NullLogger<OutboxProcessor>.Instance,
            lifetime,
            TimeSpan.FromHours(1));
        using var cts = new CancellationTokenSource();
        await processor.StartAsync(cts.Token);
        cts.Cancel();

        try
        {
            await processor.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (OperationCanceledException) when (processor.ExecuteTask!.IsCanceled)
        {
        }

        Assert.True(processor.ExecuteTask.IsCompleted);
        Assert.False(processor.ExecuteTask.IsFaulted);
        Assert.Empty(publisher.Ids);
        await processor.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Stop_between_rows_leaves_the_rest_unprocessed()
    {
        var writer = new InMemoryOutboxWriter();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var publisher = new GatePublisher(entered, release);
        var processor = new TestableOutboxProcessor(writer, publisher, batchSize: 10);
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        await writer.WriteAsync(Row(first, 1));
        await writer.WriteAsync(Row(second, 2));

        var batch = processor.ProcessBatchPublicAsync(CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        processor.RequestStop();
        release.TrySetResult();
        await batch.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal([first], publisher.Ids);
        var rows = writer.Snapshot().OrderBy(row => row.SequencePosition).ToArray();
        Assert.NotNull(rows[0].ProcessedAt);
        Assert.Null(rows[1].ProcessedAt);
        Assert.Equal(0, rows[1].Attempts);
        Assert.Null(rows[1].DeadLetteredAt);
    }

    [Fact]
    public async Task Shutdown_wait_ends_when_the_host_token_is_cancelled()
    {
        var writer = new InMemoryOutboxWriter();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var publisher = new GatePublisher(entered, release);
        var logs = new ListLogger();
        var lifetime = new LifetimeStub(started: true);
        var processor = new OutboxProcessor(
            writer,
            publisher,
            logs,
            lifetime,
            TimeSpan.FromHours(1));
        await writer.WriteAsync(Row(Guid.NewGuid(), 1));
        await processor.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        processor.RequestStop();
        using var shutdown = new CancellationTokenSource();
        var waiting = processor.WaitUntilIdleAsync(shutdown.Token);
        Assert.False(waiting.IsCompleted);
        shutdown.Cancel();
        await waiting.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Contains(logs.Lines, line => line.Contains("shutdown timed out", StringComparison.Ordinal));
        release.TrySetResult();
        await processor.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Unexpected_cancellation_during_a_batch_does_not_stop_the_loop()
    {
        var inner = new InMemoryOutboxWriter();
        var writer = new CancelOnceOnReadWriter(inner);
        var publisher = new RecordingPublisher();
        var processor = new OutboxProcessor(
            writer,
            publisher,
            NullLogger<OutboxProcessor>.Instance,
            TimeSpan.FromMilliseconds(20));
        var id = Guid.NewGuid();
        await inner.WriteAsync(Row(id, 1));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await processor.StartAsync(CancellationToken.None);

        while (publisher.Ids.Count == 0)
            await Task.Delay(10, cts.Token);

        Assert.False(processor.ExecuteTask!.IsFaulted);
        Assert.Equal(id, Assert.Single(publisher.Ids));
        Assert.NotNull(inner.Snapshot().Single(row => row.Id == id).ProcessedAt);
        await processor.StopAsync(CancellationToken.None);
    }

    private static OutboxMessage Row(Guid id, long sequence) => new()
    {
        Id = id,
        EventType = "shutdown.ping",
        Payload = "{}"u8.ToArray(),
        Metadata = "{}",
        CreatedAt = DateTime.UtcNow,
        StreamId = "shutdown",
        SequencePosition = sequence
    };

    private sealed class LifetimeStub : IHostApplicationLifetime
    {
        private readonly CancellationTokenSource _started = new();
        private readonly CancellationTokenSource _stopping = new();
        private readonly CancellationTokenSource _stopped = new();

        public LifetimeStub(bool started)
        {
            if (started)
                _started.Cancel();
        }

        public CancellationToken ApplicationStarted => _started.Token;

        public CancellationToken ApplicationStopping => _stopping.Token;

        public CancellationToken ApplicationStopped => _stopped.Token;

        public void StopApplication() => _stopping.Cancel();
    }

    private sealed class RecordingPublisher : IOutboxPublisher
    {
        public List<Guid> Ids { get; } = [];

        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            Ids.Add(message.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class GatePublisher : IOutboxPublisher
    {
        private readonly TaskCompletionSource _entered;
        private readonly TaskCompletionSource _release;
        private int _calls;

        public GatePublisher(TaskCompletionSource entered, TaskCompletionSource release)
        {
            _entered = entered;
            _release = release;
        }

        public List<Guid> Ids { get; } = [];

        public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            Ids.Add(message.Id);
            if (Interlocked.Increment(ref _calls) == 1)
            {
                _entered.TrySetResult();
                await _release.Task;
            }
        }
    }

    private sealed class CancelOnceOnReadWriter : IOutboxWriter
    {
        private readonly InMemoryOutboxWriter _inner;
        private int _reads;

        public CancelOnceOnReadWriter(InMemoryOutboxWriter inner) => _inner = inner;

        public Task<IReadOnlyList<OutboxMessage>> GetUnprocessedAsync(int batchSize, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _reads) == 1)
                throw new OperationCanceledException();

            return _inner.GetUnprocessedAsync(batchSize, cancellationToken);
        }

        public Task WriteAsync(OutboxMessage message, CancellationToken cancellationToken = default)
            => _inner.WriteAsync(message, cancellationToken);

        public Task WriteBatchAsync(IEnumerable<OutboxMessage> messages, CancellationToken cancellationToken = default)
            => _inner.WriteBatchAsync(messages, cancellationToken);

        public Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
            => _inner.MarkAsProcessedAsync(messageId, cancellationToken);

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

    private sealed class ListLogger : ILogger<OutboxProcessor>
    {
        private readonly List<string> _lines = [];

        public IReadOnlyList<string> Lines
        {
            get
            {
                lock (_lines)
                    return _lines.ToArray();
            }
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => Empty.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
                _lines.Add(formatter(state, exception));
        }

        private sealed class Empty : IDisposable
        {
            public static readonly Empty Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
