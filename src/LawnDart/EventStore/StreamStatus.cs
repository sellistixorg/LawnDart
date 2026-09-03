namespace LawnDart.EventStore;

/// <summary>
/// Status of an event stream.
/// </summary>
public enum StreamStatus
{
    /// <summary>
    /// Stream is active and receiving events.
    /// </summary>
    Active,

    /// <summary>
    /// Stream is archived (read-only, no new events).
    /// </summary>
    Archived,

    /// <summary>
    /// Stream is soft-deleted (events remain but stream is marked deleted).
    /// </summary>
    Deleted
}


