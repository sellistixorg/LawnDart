using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;

namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Resolves view-store instance identifiers for Lightweight projections.
/// </summary>
/// <remarks>
/// <para>
/// Unpartitioned kinds (<see cref="ProjectionKind.Global"/> and <see cref="ProjectionKind.Dcb"/>)
/// use a single logical view per node. When <c>TotalInstances == 1</c> the id is
/// <see cref="UnpartitionedBase"/> (<c>"global"</c>). When <c>TotalInstances &gt; 1</c> each node
/// writes and reads its own replica: <c>global:n{NodeInstance}</c> (Q5b — full replica per node,
/// not a race on one shared row).
/// </para>
/// </remarks>
public static class ProjectionInstanceIds
{
    /// <summary>
    /// Base instance id for unpartitioned projections on a single-node deployment.
    /// </summary>
    public const string UnpartitionedBase = "global";

    /// <summary>
    /// Returns whether <paramref name="kind"/> uses a single unpartitioned view instance
    /// (Global / Dcb runner path).
    /// </summary>
    public static bool IsUnpartitionedKind(ProjectionKind kind) =>
        kind is ProjectionKind.Global or ProjectionKind.Dcb;

    /// <summary>
    /// Resolves the view instance id for an unpartitioned (Global / Dcb) projection on this node.
    /// </summary>
    /// <param name="nodeInstance">Zero-based node index.</param>
    /// <param name="totalInstances">Cluster size; values ≤ 1 yield <see cref="UnpartitionedBase"/>.</param>
    public static string ForUnpartitioned(int nodeInstance, int totalInstances)
    {
        if (totalInstances <= 1)
            return UnpartitionedBase;

        if (nodeInstance < 0 || nodeInstance >= totalInstances)
        {
            throw new ArgumentOutOfRangeException(
                nameof(nodeInstance),
                nodeInstance,
                $"NodeInstance must be in [0, {totalInstances - 1}] when TotalInstances is {totalInstances}.");
        }

        return $"{UnpartitionedBase}:n{nodeInstance}";
    }

    /// <summary>
    /// Resolves the unpartitioned instance id from <see cref="IPartitioningService"/> node settings.
    /// </summary>
    public static string ForUnpartitioned(IPartitioningService partitioning)
    {
        ArgumentNullException.ThrowIfNull(partitioning);
        return ForUnpartitioned(partitioning.NodeInstance, partitioning.TotalInstances);
    }

    /// <summary>
    /// Resolves the unpartitioned instance id from <see cref="LightweightProjectionOptions"/>.
    /// </summary>
    public static string ForUnpartitioned(LightweightProjectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return ForUnpartitioned(options.NodeInstance, options.TotalInstances);
    }
}
