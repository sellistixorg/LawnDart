namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// The runner lost the right to save, or the checkpoint row no longer has the
/// epoch this copy started with. The checkpoint was not saved.
/// </summary>
public sealed class ProjectionLeadershipLostException : InvalidOperationException
{
    /// <summary>
    /// Creates the exception for a runner that lost the lead and will try to take it again.
    /// </summary>
    /// <param name="projection">Projection display name.</param>
    /// <param name="nodeId">Node number this copy was configured with.</param>
    /// <param name="epoch">Epoch this copy started with, when one was stored.</param>
    public ProjectionLeadershipLostException(string projection, int nodeId, long? epoch)
        : base(MessageFor(projection, nodeId, epoch))
    {
        Projection = projection;
        NodeId = nodeId;
        Epoch = epoch;
    }

    /// <summary>
    /// Creates the exception from a store that rejected the save.
    /// </summary>
    /// <param name="message">What the store refused.</param>
    /// <param name="inner">The store error.</param>
    public ProjectionLeadershipLostException(string message, Exception inner)
        : base(message, inner)
    {
        Projection = string.Empty;
        NodeId = -1;
    }

    /// <summary>Projection name. Empty when the store threw without a runner.</summary>
    public string Projection { get; }

    /// <summary>Node number. <c>-1</c> when the store threw without a runner.</summary>
    public int NodeId { get; }

    /// <summary>Epoch this copy started with, when one was stored.</summary>
    public long? Epoch { get; }

    private static string MessageFor(string projection, int nodeId, long? epoch)
    {
        var epochText = epoch is null
            ? string.Empty
            : $" This copy started with epoch {epoch.Value}.";
        return $"The runner for {projection} on node {nodeId} lost the lead.{epochText} The checkpoint was not saved.";
    }
}
