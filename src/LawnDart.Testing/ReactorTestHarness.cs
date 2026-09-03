using Microsoft.Extensions.Options;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.Reaction;

namespace LawnDart.Testing;

/// <summary>
/// Test harness for driving an <see cref="IReactor{TEvent}"/> in isolation.
/// Uses an in-memory inbox store so idempotency behaviour is fully exercised
/// without a broker or database. Suitable for unit tests and integration tests.
/// </summary>
/// <typeparam name="TReactor">The reactor implementation under test.</typeparam>
/// <typeparam name="TEvent">The event type the reactor handles.</typeparam>
public sealed class ReactorTestHarness<TReactor, TEvent>
    where TReactor : class, IReactor<TEvent>
    where TEvent : class, IEvent
{
    private readonly TReactor _reactor;
    private readonly InMemoryInboxStore _inboxStore;

    /// <param name="reactor">The reactor instance under test.</param>
    /// <param name="options">Optional messaging options (controls deduplication window).</param>
    public ReactorTestHarness(TReactor reactor, MessagingOptions? options = null)
    {
        _reactor = reactor;
        _inboxStore = new InMemoryInboxStore(
            Options.Create(options ?? new MessagingOptions()));
    }

    /// <summary>
    /// Delivers an event to the reactor, applying inbox deduplication.
    /// If <paramref name="context"/> carries a <see cref="MessageContext.MessageId"/> that has
    /// already been processed, an empty list is returned without invoking the reactor.
    /// </summary>
    /// <param name="event">The event to deliver.</param>
    /// <param name="context">
    /// Correlation metadata. If null, a new context with a generated <see cref="MessageContext.MessageId"/> is created.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Commands emitted by the reactor, or empty if deduplicated.</returns>
    public async Task<IReadOnlyList<ICommand>> ReactAsync(
        TEvent @event,
        MessageContext? context = null,
        CancellationToken cancellationToken = default)
    {
        context ??= MessageContext.New();

        if (context.MessageId is not null && await _inboxStore.IsProcessedAsync(context.MessageId, cancellationToken))
            return [];

        var commands = (await _reactor.ReactAsync(@event, context, cancellationToken)).ToList();

        if (context.MessageId is not null)
            await _inboxStore.MarkProcessedAsync(context.MessageId, DateTimeOffset.UtcNow, cancellationToken);

        return commands;
    }

    /// <summary>
    /// Returns true if a message with the given ID has already been processed by this harness.
    /// </summary>
    public Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default)
        => _inboxStore.IsProcessedAsync(messageId, cancellationToken);

    /// <summary>
    /// Clears the inbox store. Call between test cases that should not share deduplication state.
    /// </summary>
    public void Reset() => _inboxStore.Clear();

    /// <summary>
    /// Number of message IDs currently tracked in the inbox.
    /// </summary>
    public int InboxCount => _inboxStore.Count;
}
