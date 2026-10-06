using LawnDart.EventSourcing.Outbox;
using LawnDart.EventSourcing.SqlServer.Outbox;
using LawnDart.Outbox;
using Microsoft.Extensions.Logging.Abstractions;

namespace LawnDart.EventSourcing.SqlServer.Tests.Outbox;

[Trait("Category", "Integration")]
public class SqlServerOutboxDeadLetterResetTests : IClassFixture<SqlServerOutboxResetFixture>
{
    private readonly SqlServerOutboxResetFixture _fixture;

    public SqlServerOutboxDeadLetterResetTests(SqlServerOutboxResetFixture fixture)
        => _fixture = fixture;

    [Fact]
    public async Task ResetDeadLettered_ClearsDeadLetter_KeepsLastError_AndZeroesAttempts()
    {
        var writer = await NewWriterAsync();
        var message = NewMessage(attempts: 4);
        await writer.WriteAsync(message);
        await writer.RecordFailureAsync(message.Id, "broker down");
        await writer.MarkAsDeadLetteredAsync(message.Id);

        var before = Assert.Single(await writer.GetDeadLetteredAsync(10));

        Assert.True(await writer.ResetDeadLetteredAsync(message.Id));

        var queued = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(message.Id, queued.Id);
        Assert.Equal(0, queued.Attempts);
        Assert.Null(queued.DeadLetteredAt);
        Assert.Null(queued.ProcessedAt);
        Assert.Equal("broker down", queued.LastError);
        Assert.Equal(before.LastAttemptAt, queued.LastAttemptAt);
        Assert.Empty(await writer.GetDeadLetteredAsync(10));
    }

    [Fact]
    public async Task ResetDeadLettered_Unknown_NeverDeadLettered_AndProcessed_ReturnFalse()
    {
        var writer = await NewWriterAsync();
        Assert.False(await writer.ResetDeadLetteredAsync(Guid.NewGuid()));

        var live = NewMessage(sequence: 1, attempts: 2);
        await writer.WriteAsync(live);
        await writer.RecordFailureAsync(live.Id, "once");
        Assert.False(await writer.ResetDeadLetteredAsync(live.Id));
        var stillLive = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(3, stillLive.Attempts);
        Assert.Equal("once", stillLive.LastError);

        var done = NewMessage(sequence: 2, attempts: 6);
        await writer.WriteAsync(done);
        await writer.MarkAsDeadLetteredAsync(done.Id);
        await writer.MarkAsProcessedAsync(done.Id);
        Assert.False(await writer.ResetDeadLetteredAsync(done.Id));
        Assert.DoesNotContain(await writer.GetUnprocessedAsync(10), m => m.Id == done.Id);
        var stillDead = (await writer.GetDeadLetteredAsync(10)).Single(m => m.Id == done.Id);
        Assert.Equal(6, stillDead.Attempts);
        Assert.NotNull(stillDead.ProcessedAt);
        Assert.NotNull(stillDead.DeadLetteredAt);

        Assert.False(await writer.ResetDeadLetteredAsync(live.Id));
    }

    [Fact]
    public async Task ResetDeadLettered_SecondCall_ReturnsFalse()
    {
        var writer = await NewWriterAsync();
        var message = NewMessage(attempts: 3);
        await writer.WriteAsync(message);
        await writer.MarkAsDeadLetteredAsync(message.Id);

        Assert.True(await writer.ResetDeadLetteredAsync(message.Id));
        Assert.False(await writer.ResetDeadLetteredAsync(message.Id));

        var queued = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(0, queued.Attempts);
    }

    [Fact]
    public async Task ResetAll_ReturnsCount_AndSkipsLiveAndProcessedRows()
    {
        var writer = await NewWriterAsync();
        var deadA = NewMessage(sequence: 1, attempts: 5);
        var deadB = NewMessage(sequence: 2, attempts: 8);
        var deadC = NewMessage(sequence: 3, attempts: 9);
        var live = NewMessage(sequence: 4, attempts: 2);
        var processed = NewMessage(sequence: 5, attempts: 1);
        await writer.WriteBatchAsync([deadA, deadB, deadC, live, processed]);
        await writer.MarkAsDeadLetteredAsync(deadA.Id);
        await writer.MarkAsDeadLetteredAsync(deadB.Id);
        await writer.MarkAsDeadLetteredAsync(deadC.Id);
        await writer.MarkAsProcessedAsync(processed.Id);

        Assert.Equal(3, await writer.ResetAllDeadLetteredAsync());
        Assert.Equal(0, await writer.ResetAllDeadLetteredAsync());

        var queued = await writer.GetUnprocessedAsync(10);
        Assert.Equal(4, queued.Count);
        Assert.All(queued.Where(m => m.Id != live.Id), m => Assert.Equal(0, m.Attempts));
        Assert.Equal(2, queued.Single(m => m.Id == live.Id).Attempts);
        Assert.DoesNotContain(queued, m => m.Id == processed.Id);
        Assert.Empty(await writer.GetDeadLetteredAsync(10));
    }

    [Fact]
    public async Task Reset_ThenOutboxProcessor_PublishesAndMarksProcessed()
    {
        var writer = await NewWriterAsync();
        var publisher = new ResetTestPublisher { ShouldFail = true };
        var message = NewMessage();
        await writer.WriteAsync(message);

        var processor = new ResetTestProcessor(writer, publisher, maxAttempts: 3);
        for (var i = 0; i < 6; i++)
        {
            await processor.ProcessBatchAsync();
            if ((await writer.GetDeadLetteredAsync(10)).Any(m => m.Id == message.Id))
                break;
        }

        Assert.Contains(await writer.GetDeadLetteredAsync(10), m => m.Id == message.Id);
        Assert.True(await writer.ResetDeadLetteredAsync(message.Id));
        var queued = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(0, queued.Attempts);
        Assert.Null(queued.DeadLetteredAt);

        publisher.ShouldFail = false;
        await processor.ProcessBatchAsync();

        Assert.Contains(publisher.Published, m => m.Id == message.Id);
        Assert.DoesNotContain(await writer.GetUnprocessedAsync(10), m => m.Id == message.Id);
        Assert.DoesNotContain(await writer.GetDeadLetteredAsync(10), m => m.Id == message.Id);
    }

    private async Task<SqlServerOutboxWriter> NewWriterAsync()
    {
        var table = "Outbox" + Guid.NewGuid().ToString("N");
        var writer = new SqlServerOutboxWriter(
            _fixture.ConnectionString,
            table,
            NullLogger<SqlServerOutboxWriter>.Instance);
        await writer.InitializeSchemaAsync();
        return writer;
    }

    private static OutboxMessage NewMessage(long sequence = 1, int attempts = 0)
        => new()
        {
            Id = Guid.NewGuid(),
            EventType = "TestEvent",
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            StreamId = "stream1",
            SequencePosition = sequence,
            Attempts = attempts
        };

    private sealed class ResetTestPublisher : IOutboxPublisher
    {
        public bool ShouldFail { get; set; }
        public List<OutboxMessage> Published { get; } = [];

        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            if (ShouldFail)
                throw new InvalidOperationException("Simulated publish failure");

            Published.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class ResetTestProcessor : OutboxProcessor
    {
        public ResetTestProcessor(IOutboxWriter writer, IOutboxPublisher publisher, int maxAttempts)
            : base(
                writer,
                publisher,
                NullLogger<OutboxProcessor>.Instance,
                TimeSpan.FromSeconds(5),
                batchSize: 100,
                maxAttempts)
        {
        }

        public Task ProcessBatchAsync()
        {
            var method = typeof(OutboxProcessor).GetMethod(
                "ProcessBatchAsync",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return (Task)method!.Invoke(this, [CancellationToken.None])!;
        }
    }
}
