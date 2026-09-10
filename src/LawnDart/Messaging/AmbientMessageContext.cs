namespace LawnDart.Messaging;

/// <summary>
/// Publishes the inbound <see cref="MessageContext"/> for the current async flow
/// so command metadata capture can read it without changing
/// <c>ICommandHandler&lt;T&gt;</c>.
/// </summary>
public static class AmbientMessageContext
{
    private static readonly AsyncLocal<MessageContext?> CurrentContext = new();

    /// <summary>The inbound context for this async flow, or <c>null</c>.</summary>
    public static MessageContext? Current => CurrentContext.Value;

    /// <summary>
    /// Sets <see cref="Current"/> until the returned scope is disposed.
    /// Nested pushes restore the previous value.
    /// </summary>
    public static IDisposable Push(MessageContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var previous = CurrentContext.Value;
        CurrentContext.Value = context;
        return new Scope(previous);
    }

    private sealed class Scope : IDisposable
    {
        private readonly MessageContext? _previous;
        private bool _disposed;

        internal Scope(MessageContext? previous) => _previous = previous;

        public void Dispose()
        {
            if (_disposed)
                return;
            CurrentContext.Value = _previous;
            _disposed = true;
        }
    }
}
