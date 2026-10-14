using System.Runtime.ExceptionServices;

namespace LawnDart.Messaging;

/// <summary>
/// Result of one delivery attempt.
/// </summary>
public sealed class MessageAttemptResult
{
    private MessageAttemptResult(bool succeeded, Exception? error)
    {
        Succeeded = succeeded;
        Error = error;
    }

    /// <summary><c>true</c> when the handler finished without throwing.</summary>
    public bool Succeeded { get; }

    /// <summary>The failure from this attempt, or <c>null</c> when <see cref="Succeeded"/> is <c>true</c>.</summary>
    public Exception? Error { get; }

    /// <summary>The handler finished.</summary>
    public static MessageAttemptResult Handled() => new(true, null);

    /// <summary>The handler or a pre-handler hook failed.</summary>
    /// <param name="error">The exception from this attempt.</param>
    public static MessageAttemptResult Failed(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new(false, error);
    }
}

/// <summary>
/// Runs the pre-handler hook, then the message handler, and applies retry and dead-letter rules.
/// </summary>
public static class MessageDelivery
{
    /// <summary>
    /// Invokes <paramref name="preHandlers"/> and then <paramref name="handler"/> once.
    /// Cancellation requested through <paramref name="cancellationToken"/> is not a failed attempt.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="message">The message body.</param>
    /// <param name="context">Context passed to the hook and the handler.</param>
    /// <param name="handler">The message handler.</param>
    /// <param name="preHandlers">Hooks to run first, in order. May be empty.</param>
    /// <param name="cancellationToken">Cancellation token for this attempt.</param>
    public static async Task<MessageAttemptResult> TryOnceAsync<TMessage>(
        TMessage message,
        MessageContext context,
        Func<TMessage, MessageContext, CancellationToken, Task> handler,
        IReadOnlyList<IMessagePreHandler> preHandlers,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(preHandlers);

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            foreach (var preHandler in preHandlers)
            {
                await preHandler.BeforeHandleAsync(context, cancellationToken).ConfigureAwait(false);
            }

            await handler(message, context, cancellationToken).ConfigureAwait(false);
            return MessageAttemptResult.Handled();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return MessageAttemptResult.Failed(ex);
        }
    }

    /// <summary>
    /// Delivers <paramref name="message"/> up to <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>.
    /// Each attempt runs the pre-handler hook before the handler.
    /// When the attempts are used and <see cref="MessageTransportOptions.DeadLetterFailedMessages"/> is
    /// <c>true</c>, the message is parked on <paramref name="deadLetters"/> and this method returns.
    /// When that flag is <c>false</c>, the last failure is thrown.
    /// A cancelled token stops delivery and does not dead-letter the message.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="message">The message body.</param>
    /// <param name="context">Context passed to the hook and the handler.</param>
    /// <param name="handler">The message handler.</param>
    /// <param name="options">Retry and dead-letter settings.</param>
    /// <param name="preHandlers">Hooks to run first, in order. May be empty.</param>
    /// <param name="deadLetters">Queue used when dead-letter handling is on.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task DeliverAsync<TMessage>(
        TMessage message,
        MessageContext context,
        Func<TMessage, MessageContext, CancellationToken, Task> handler,
        MessageTransportOptions options,
        IReadOnlyList<IMessagePreHandler> preHandlers,
        IDeadLetterQueue deadLetters,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(deadLetters);
        options.EnsureValid();

        var messageType = MessageTypeNames.Require(typeof(TMessage));
        Exception? last = null;

        for (var attempt = 1; attempt <= options.MaxDeliveryAttempts; attempt++)
        {
            var result = await TryOnceAsync(
                message,
                context,
                handler,
                preHandlers,
                cancellationToken).ConfigureAwait(false);

            if (result.Succeeded)
                return;

            last = result.Error;
        }

        if (options.DeadLetterFailedMessages)
        {
            var error = last is null
                ? "Delivery failed."
                : $"{last.GetType().Name}: {last.Message}";
            deadLetters.Enqueue(new DeadLetteredMessage(messageType, message, context, options.MaxDeliveryAttempts, error));
            return;
        }

        ExceptionDispatchInfo.Capture(last!).Throw();
    }
}
