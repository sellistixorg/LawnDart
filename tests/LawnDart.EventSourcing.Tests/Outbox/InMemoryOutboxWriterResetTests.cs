using LawnDart.EventSourcing.Outbox;
using LawnDart.Outbox;

namespace LawnDart.EventSourcing.Tests.Outbox;

public class InMemoryOutboxWriterResetTests
{
    [Fact]
    public async Task ResetDeadLettered_ClearsDeadLetter_KeepsLastError_AndZeroesAttempts()
    {
        var writer = new InMemoryOutboxWriter();
        var message = NewMessage();
        await writer.WriteAsync(message);
        await writer.RecordFailureAsync(message.Id, "broker down");
        await writer.MarkAsDeadLetteredAsync(message.Id);

        var before = Assert.Single(await writer.GetDeadLetteredAsync(10));
        Assert.NotNull(before.LastAttemptAt);

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
    public async Task ResetDeadLettered_UnknownId_ReturnsFalse()
    {
        var writer = new InMemoryOutboxWriter();
        Assert.False(await writer.ResetDeadLetteredAsync(Guid.NewGuid()));
        Assert.Equal(0, await writer.ResetAllDeadLetteredAsync());
    }

    [Fact]
    public async Task ResetDeadLettered_NeverDeadLettered_ReturnsFalse_AndLeavesAttempts()
    {
        var writer = new InMemoryOutboxWriter();
        var message = NewMessage();
        await writer.WriteAsync(message);
        await writer.RecordFailureAsync(message.Id, "once");

        Assert.False(await writer.ResetDeadLetteredAsync(message.Id));

        var queued = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(1, queued.Attempts);
        Assert.Equal("once", queued.LastError);
        Assert.Null(queued.DeadLetteredAt);
    }

    [Fact]
    public async Task ResetDeadLettered_AlreadyProcessed_ReturnsFalse()
    {
        var writer = new InMemoryOutboxWriter();
        var message = NewMessage(attempts: 4);
        await writer.WriteAsync(message);
        await writer.MarkAsDeadLetteredAsync(message.Id);
        await writer.MarkAsProcessedAsync(message.Id);

        Assert.False(await writer.ResetDeadLetteredAsync(message.Id));

        Assert.Empty(await writer.GetUnprocessedAsync(10));
        var dead = Assert.Single(await writer.GetDeadLetteredAsync(10));
        Assert.Equal(4, dead.Attempts);
        Assert.NotNull(dead.DeadLetteredAt);
        Assert.NotNull(dead.ProcessedAt);
    }

    [Fact]
    public async Task ResetDeadLettered_SecondCall_ReturnsFalse()
    {
        var writer = new InMemoryOutboxWriter();
        var message = NewMessage(attempts: 3);
        await writer.WriteAsync(message);
        await writer.MarkAsDeadLetteredAsync(message.Id);

        Assert.True(await writer.ResetDeadLetteredAsync(message.Id));
        Assert.False(await writer.ResetDeadLetteredAsync(message.Id));

        var queued = Assert.Single(await writer.GetUnprocessedAsync(10));
        Assert.Equal(0, queued.Attempts);
    }

    [Fact]
    public async Task ResetAll_ResetsOnlyDeadLetteredUnprocessedRows()
    {
        var writer = new InMemoryOutboxWriter();
        var deadA = NewMessage(sequence: 1, attempts: 5);
        var deadB = NewMessage(sequence: 2, attempts: 8);
        var live = NewMessage(sequence: 3, attempts: 2);
        var processed = NewMessage(sequence: 4, attempts: 1);
        await writer.WriteAsync(deadA);
        await writer.WriteAsync(deadB);
        await writer.WriteAsync(live);
        await writer.WriteAsync(processed);
        await writer.MarkAsDeadLetteredAsync(deadA.Id);
        await writer.MarkAsDeadLetteredAsync(deadB.Id);
        await writer.MarkAsProcessedAsync(processed.Id);

        Assert.Equal(2, await writer.ResetAllDeadLetteredAsync());
        Assert.Equal(0, await writer.ResetAllDeadLetteredAsync());

        var queued = await writer.GetUnprocessedAsync(10);
        Assert.Equal(3, queued.Count);
        Assert.Equal(0, queued.Single(m => m.Id == deadA.Id).Attempts);
        Assert.Equal(0, queued.Single(m => m.Id == deadB.Id).Attempts);
        Assert.Null(queued.Single(m => m.Id == deadA.Id).DeadLetteredAt);
        Assert.Equal(2, queued.Single(m => m.Id == live.Id).Attempts);
        Assert.Empty(await writer.GetDeadLetteredAsync(10));
    }

    [Fact]
    public async Task Reset_HonorsCancellation()
    {
        var writer = new InMemoryOutboxWriter();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => writer.ResetDeadLetteredAsync(Guid.NewGuid(), cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => writer.ResetAllDeadLetteredAsync(cts.Token));
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
}
