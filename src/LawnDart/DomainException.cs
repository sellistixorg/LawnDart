namespace LawnDart;

/// <summary>
/// Exception thrown when a command or domain operation is rejected because it violates
/// a business rule, invariant, or current domain state.
/// </summary>
/// <remarks>
/// <para>
/// This type deliberately derives from <see cref="InvalidOperationException"/> so existing
/// domain code can migrate incrementally from broad invalid-operation failures to an
/// explicit business exception contract without changing catch semantics everywhere at once.
/// </para>
/// <para>
/// HTTP command endpoints can map <see cref="DomainException"/> to a client-facing
/// <c>422 Unprocessable Entity</c> response while still allowing unrelated server faults
/// to bubble through as <c>500</c>.
/// </para>
/// </remarks>
public class DomainException : InvalidOperationException
{
    /// <summary>
    /// Gets the problem title that should be used when this exception is translated into
    /// a client-facing error response.
    /// </summary>
    public string Title { get; }

    public DomainException(string message, string title = "Domain rule violated")
        : base(message)
    {
        Title = title;
    }

    public DomainException(string message, Exception innerException, string title = "Domain rule violated")
        : base(message, innerException)
    {
        Title = title;
    }
}
