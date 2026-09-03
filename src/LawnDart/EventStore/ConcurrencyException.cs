namespace LawnDart.EventStore;

/// <summary>
/// Exception thrown when an append operation fails due to a concurrency conflict.
/// </summary>
public class ConcurrencyException : Exception
{
    /// <summary>
    /// The expected version or sequence position that conflicted.
    /// </summary>
    public long? ExpectedPosition { get; }

    /// <summary>
    /// The actual version or sequence position found.
    /// </summary>
    public long? ActualPosition { get; }

    public ConcurrencyException(string message, long? expectedPosition = null, long? actualPosition = null)
        : base(message)
    {
        ExpectedPosition = expectedPosition;
        ActualPosition = actualPosition;
    }

    public ConcurrencyException(string message, Exception innerException, long? expectedPosition = null, long? actualPosition = null)
        : base(message, innerException)
    {
        ExpectedPosition = expectedPosition;
        ActualPosition = actualPosition;
    }
}


