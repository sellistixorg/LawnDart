using Microsoft.Extensions.Logging;

namespace LawnDart.Projections.Lightweight.Postgres;

internal static partial class PostgresProjectionLog
{
    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Two copies of projection {Projection} share node number {Node}. This copy is waiting. Give each copy its own node number.")]
    internal static partial void WaitingForNode(ILogger logger, string projection, int node);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Warning,
        Message = "Projection {Projection} node {Node} is using a lease. Failover is slower in this mode: another copy can take over after the lease expires ({LeaseSeconds} seconds). Point ListenConnectionString at Postgres directly, or at a session pooler, to take over as soon as the connection drops.")]
    internal static partial void LeaseMode(ILogger logger, string projection, int node, double leaseSeconds);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Warning,
        Message = "Projection {Projection} node {Node} lost its lock. This copy will try to take the lead again.")]
    internal static partial void LockLost(ILogger logger, string projection, int node);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Information,
        Message = "Initialized Postgres projection tables in schema {Schema}.")]
    internal static partial void SchemaReady(ILogger logger, string schema);
}
