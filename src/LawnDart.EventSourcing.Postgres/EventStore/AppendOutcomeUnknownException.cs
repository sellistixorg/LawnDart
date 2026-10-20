namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// The append statement was sent, and a later read could not tell whether it was saved.
/// </summary>
public sealed class AppendOutcomeUnknownException : InvalidOperationException
{
    /// <summary>Creates the exception.</summary>
    public AppendOutcomeUnknownException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with the error from the failed read.</summary>
    public AppendOutcomeUnknownException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
