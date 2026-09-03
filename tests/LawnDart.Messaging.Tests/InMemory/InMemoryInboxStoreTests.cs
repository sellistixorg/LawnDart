using Microsoft.Extensions.Options;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;

namespace LawnDart.Messaging.Tests.InMemory;

public class InMemoryInboxStoreTests
{
    private readonly InMemoryInboxStore _store =
        new(Options.Create(new MessagingOptions()));

    [Fact]
    public async Task IsProcessedAsync_UnknownId_ReturnsFalse()
    {
        var result = await _store.IsProcessedAsync("unknown-id");
        Assert.False(result);
    }

    [Fact]
    public async Task MarkProcessed_ThenIsProcessed_ReturnsTrue()
    {
        const string messageId = "msg-1";
        await _store.MarkProcessedAsync(messageId, DateTimeOffset.UtcNow);

        var result = await _store.IsProcessedAsync(messageId);
        Assert.True(result);
    }

    [Fact]
    public async Task MarkProcessed_Idempotent_DoesNotThrow()
    {
        const string messageId = "msg-dup";
        await _store.MarkProcessedAsync(messageId, DateTimeOffset.UtcNow);

        var ex = await Record.ExceptionAsync(() =>
            _store.MarkProcessedAsync(messageId, DateTimeOffset.UtcNow));
        Assert.Null(ex);
    }

    [Fact]
    public async Task IsProcessedAsync_ExpiredEntry_ReturnsFalse()
    {
        // Use a very short deduplication window.
        var store = new InMemoryInboxStore(Options.Create(new MessagingOptions
        {
            InboxDeduplicationWindow = TimeSpan.FromMilliseconds(1)
        }));

        const string messageId = "msg-expiring";
        await store.MarkProcessedAsync(messageId, DateTimeOffset.UtcNow - TimeSpan.FromSeconds(1));

        var result = await store.IsProcessedAsync(messageId);
        Assert.False(result);
    }

    [Fact]
    public async Task Clear_RemovesAllEntries()
    {
        await _store.MarkProcessedAsync("a", DateTimeOffset.UtcNow);
        await _store.MarkProcessedAsync("b", DateTimeOffset.UtcNow);
        Assert.Equal(2, _store.Count);

        _store.Clear();

        Assert.Equal(0, _store.Count);
        Assert.False(await _store.IsProcessedAsync("a"));
        Assert.False(await _store.IsProcessedAsync("b"));
    }

    [Fact]
    public async Task Count_TracksCurrentEntries()
    {
        Assert.Equal(0, _store.Count);
        await _store.MarkProcessedAsync("x", DateTimeOffset.UtcNow);
        Assert.Equal(1, _store.Count);
        await _store.MarkProcessedAsync("y", DateTimeOffset.UtcNow);
        Assert.Equal(2, _store.Count);
    }
}
