using Microsoft.Extensions.Options;
using LawnDart.Messaging;
using LawnDart.Messaging.InMemory;
using LawnDart.Patterns.EventProcessing;

namespace LawnDart.Testing;

/// <summary>
/// Test harness for driving an <see cref="IEventProcessor{TEvent}"/> in isolation.
/// Applies inbox deduplication so idempotency behaviour is fully exercised
/// without a broker or database. Suitable for unit tests and integration tests.
/// </summary>
/// <typeparam name="TProcessor">The event processor implementation under test.</typeparam>
/// <typeparam name="TEvent">The input event type the processor handles.</typeparam>
public sealed class EventProcessorTestHarness<TProcessor, TEvent>
    where TProcessor : class, IEventProcessor<TEvent>
    where TEvent : class, IEvent
{
    private readonly TProcessor _processor;
    private readonly InMemoryInboxStore _inboxStore;

    /// <param name="processor">The event processor instance under test.</param>
    /// <param name="options">Optional messaging options (controls deduplication window).</param>
    public EventProcessorTestHarness(TProcessor processor, MessagingOptions? options = null)
    {
        _processor = processor;
        _inboxStore = new InMemoryInboxStore(
            Options.Create(options ?? new MessagingOptions()));
    }

    /// <summary>
    /// Delivers an event to the processor, applying inbox deduplication.
    /// If <paramref name="context"/> carries a <see cref="MessageContext.MessageId"/> that has
    /// already been processed, an empty list is returned without invoking the processor.
    /// </summary>
    /// <param name="event">The input event to process.</param>
    /// <param name="context">
    /// Correlation metadata. If null, a new context with a generated <see cref="MessageContext.MessageId"/> is created.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Derived events emitted by the processor, or empty if deduplicated.</returns>
    public async Task<IReadOnlyList<IEvent>> ProcessAsync(
        TEvent @event,
        MessageContext? context = null,
        CancellationToken cancellationToken = default)
    {
        context ??= MessageContext.New();

        if (context.MessageId is not null && await _inboxStore.IsProcessedAsync(context.MessageId, cancellationToken))
            return [];

        var events = (await _processor.ProcessAsync(@event, context, cancellationToken)).ToList();

        if (context.MessageId is not null)
            await _inboxStore.MarkProcessedAsync(context.MessageId, DateTimeOffset.UtcNow, cancellationToken);

        return events;
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
