namespace LawnDart.Projections.Lightweight.Postgres;

/// <summary>
/// Configuration for the Postgres view store, checkpoint store, and single-runner lock.
/// </summary>
public sealed class PostgresProjectionStoreOptions
{
    /// <summary>
    /// Connection string for view and checkpoint reads and writes. A pooled connection is fine.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Optional direct connection for the single-runner lock.
    /// When null, the lock uses <see cref="ConnectionString"/>.
    /// A transaction pooler cannot keep a session lock. Point this at Postgres
    /// directly, or at a session pooler, when you want the waiting copy to take
    /// over as soon as the holder's connection drops.
    /// </summary>
    public string? ListenConnectionString { get; set; }

    /// <summary>
    /// Optional owner connection used only to create tables.
    /// When null, <see cref="ConnectionString"/> creates the schema.
    /// </summary>
    public string? MigrationConnectionString { get; set; }

    /// <summary>
    /// Postgres schema for the view, checkpoint, and lease tables.
    /// <c>AddPostgresProjectionStores</c> sets this to <c>lawndart</c> for the
    /// <c>default</c> context and to the context name otherwise. The callback can change it.
    /// Never <c>public</c>.
    /// </summary>
    public string SchemaName { get; set; } = "lawndart";

    /// <summary>
    /// When true (the default), one copy runs each projection for each node number.
    /// Set false to let every copy run, which is the behavior without this package.
    /// </summary>
    public bool EnableSingleRunnerLock { get; set; } = true;

    /// <summary>
    /// How often the lock holder checks that its session is still connected.
    /// Default 2 seconds. The runner stops before its next save after a failed check.
    /// </summary>
    public TimeSpan LockProbeInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How long a lease stays valid when the connection cannot hold a session lock.
    /// Default 15 seconds. Another copy can take over after this time with no renewal.
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// How often the lease holder extends <see cref="LeaseDuration"/>.
    /// Default 5 seconds. Must be shorter than <see cref="LeaseDuration"/>.
    /// </summary>
    public TimeSpan LeaseRenewInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Checks the connection string, schema, and lease timings.</summary>
    public void EnsureValid()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(ConnectionString));

        SchemaName = PgIdentifier.RequireSchema(SchemaName);

        if (LockProbeInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(LockProbeInterval),
                LockProbeInterval,
                "LockProbeInterval must be greater than zero.");
        }

        if (LeaseDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(LeaseDuration),
                LeaseDuration,
                "LeaseDuration must be greater than zero.");
        }

        if (LeaseRenewInterval <= TimeSpan.Zero || LeaseRenewInterval >= LeaseDuration)
        {
            throw new ArgumentOutOfRangeException(
                nameof(LeaseRenewInterval),
                LeaseRenewInterval,
                "LeaseRenewInterval must be greater than zero and shorter than LeaseDuration.");
        }
    }

    /// <summary>Connection used to create tables.</summary>
    internal string MigrationSource =>
        string.IsNullOrWhiteSpace(MigrationConnectionString)
            ? ConnectionString!
            : MigrationConnectionString;

    /// <summary>Connection the single-runner lock prefers.</summary>
    internal string LockSource =>
        string.IsNullOrWhiteSpace(ListenConnectionString)
            ? ConnectionString!
            : ListenConnectionString;
}
