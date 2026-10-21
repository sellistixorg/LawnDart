namespace LawnDart.Projections.Lightweight.Postgres;

/// <summary>
/// How a projection copy keeps one runner per node number.
/// </summary>
public enum PostgresRunnerLockMode
{
    /// <summary>
    /// Use a lease when the lock connection looks like a transaction pooler.
    /// Otherwise hold a session lock. This is the default.
    /// Port 6543, or a host containing <c>pooler</c> or <c>pgbouncer</c>, is treated as a pooler.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// Hold a session lock on the lock connection.
    /// A transaction pooler cannot keep that lock. Use <see cref="Lease"/> for that connection.
    /// </summary>
    SessionLock = 1,

    /// <summary>
    /// Use a lease row. Another copy can take over after the lease expires.
    /// Failover is slower than a session lock.
    /// </summary>
    Lease = 2
}
