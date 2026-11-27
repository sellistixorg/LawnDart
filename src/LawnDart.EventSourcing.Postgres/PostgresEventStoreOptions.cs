using LawnDart.EventSourcing;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.Postgres;

/// <summary>
/// Configuration for the Postgres event store.
/// </summary>
public class PostgresEventStoreOptions : EventSourcingOptions
{
    /// <summary>
    /// Bounded context name used as the DI key.
    /// Defaults to <c>default</c>.
    /// </summary>
    public string ContextName { get; set; } = "default";

    /// <summary>
    /// Postgres schema for this context's tables, sequence, and append function.
    /// <c>UsePostgres</c> sets this to <c>lawndart</c> for the <c>default</c> context
    /// and to the context name for every other context. The callback can change it.
    /// Never <c>public</c>.
    /// </summary>
    public string SchemaName { get; set; } = "lawndart";

    /// <summary>
    /// Connection string for ordinary reads and appends. A pooled connection is fine.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Optional direct connection for the wake-up listener.
    /// When null, the listener uses <see cref="ConnectionString"/>.
    /// A transaction pooler cannot keep this session. Point this at Postgres
    /// directly, or at a session pooler.
    /// </summary>
    public string? ListenConnectionString { get; set; }

    /// <summary>
    /// Optional owner connection used only to create and upgrade tables.
    /// When null, <see cref="ConnectionString"/> creates the schema.
    /// </summary>
    public string? MigrationConnectionString { get; set; }

    /// <summary>Events table name. Default <c>events</c>.</summary>
    public string EventsTableName { get; set; } = "events";

    /// <summary>Tag index table name. Default <c>event_tags</c>.</summary>
    public string EventTagsTableName { get; set; } = "event_tags";

    /// <summary>Stream registry table name. Default <c>streams</c>.</summary>
    public string StreamsTableName { get; set; } = "streams";

    /// <summary>Event name lookup table. Default <c>event_types</c>.</summary>
    public string EventTypesTableName { get; set; } = "event_types";

    /// <summary>Format stamp table. Default <c>schema_info</c>.</summary>
    public string SchemaInfoTableName { get; set; } = "schema_info";

    /// <summary>Stream snapshot table. Default <c>event_snapshots</c>. State is stored as bytes.</summary>
    public string EventSnapshotsTableName { get; set; } = "event_snapshots";

    /// <summary>Dynamic consistency boundary snapshot table. Default <c>dcb_snapshots</c>.</summary>
    public string DcbSnapshotsTableName { get; set; } = "dcb_snapshots";

    /// <summary>
    /// Keep the stream registry up to date on append. Default true.
    /// </summary>
    public bool EnableStreamRegistry { get; set; } = true;

    /// <summary>
    /// Controls how tenants share the store. Defaults to <see cref="LawnDart.Tenancy.Shared"/>.
    /// </summary>
    /// <remarks>
    /// <c>UsePostgres</c> copies <see cref="LawnDart.LawnDartOptions.Tenancy"/> onto this property
    /// before the configure callback runs. The callback can change it.
    /// <see cref="LawnDart.Tenancy.Isolated"/> is not supported in this release and is refused at startup.
    /// </remarks>
    public Tenancy Tenancy { get; set; } = Tenancy.Shared;

    /// <summary>
    /// Optional fixed tenant id when <see cref="Tenancy"/> is <see cref="LawnDart.Tenancy.Single"/>.
    /// Leave null for no tenant. Must be null when <see cref="Tenancy"/> is <see cref="LawnDart.Tenancy.Shared"/>.
    /// </summary>
    public string? SingleTenantId { get; set; }

    /// <summary>
    /// Obsolete alias for <see cref="Tenancy"/>.
    /// <c>true</c> maps to <see cref="LawnDart.Tenancy.Shared"/>; <c>false</c> maps to <see cref="LawnDart.Tenancy.Single"/>.
    /// </summary>
    [Obsolete("Use Tenancy instead. RequireTenantId = true maps to Tenancy.Shared; false maps to Tenancy.Single.")]
    public bool RequireTenantId
    {
        get => Tenancy != Tenancy.Single;
        set => Tenancy = value ? Tenancy.Shared : Tenancy.Single;
    }

    /// <summary>
    /// When true, a tenant id of <c>default</c> in any casing is rejected, and the id
    /// must match <see cref="LawnDartNames.Pattern"/>.
    /// <c>UsePostgres</c> copies <see cref="LawnDartOptions.ValidateNames"/> onto this
    /// property before the configure callback runs. The callback can change it.
    /// An empty tenant id is still rejected. A colon in a tenant id is still rejected.
    /// </summary>
    public bool ValidateNames { get; set; } = true;

    /// <summary>Outbox table name. Default <c>outbox</c>. Not partitioned.</summary>
    public string OutboxTableName { get; set; } = "outbox";

    /// <summary>
    /// The default number of positions in one events range. 100 million.
    /// </summary>
    public const long DefaultPartitionSize = 100_000_000;

    /// <summary>
    /// When true, the events and event_tags tables are split into ranges of
    /// <see cref="PartitionSize"/> positions, one child table per range.
    /// When null (the default), ranges are on. <see cref="LawnDart.Tenancy.Isolated"/> is
    /// refused at startup in this release, so in practice ranges are on for
    /// <see cref="LawnDart.Tenancy.Single"/> and <see cref="LawnDart.Tenancy.Shared"/>.
    /// Chosen when the schema is created. A later open with a different choice is rejected.
    /// </summary>
    /// <remarks>
    /// The store creates the next ranges ahead of time and keeps two empty ranges
    /// above the newest position. An append never waits on that work, and an append
    /// whose position has no range yet creates the one it needs first.
    /// These ranges are only a storage layout. They are not the projection work split by
    /// partition hash (<c>PartitionFilter</c>, <c>QueryItem.ForPartition</c>,
    /// <c>ConsistentHashPartitioningService</c>), which works the same with ranges on or off.
    /// </remarks>
    public bool? PositionPartitioning { get; set; }

    /// <summary>
    /// How many positions one events range holds. Default 100 million.
    /// Must be from 10,000 to 10^15. Chosen when the schema is created and stamped in <c>schema_info</c>.
    /// A later open with a different size is rejected. Ignored when position ranges are off.
    /// </summary>
    /// <remarks>
    /// A storage layout only. It does not change the projection work split by partition hash
    /// (<c>PartitionFilter</c>, <c>QueryItem.ForPartition</c>, <c>ConsistentHashPartitioningService</c>).
    /// </remarks>
    public long PartitionSize { get; set; } = DefaultPartitionSize;

    /// <summary>Smallest <see cref="PartitionSize"/> allowed.</summary>
    internal const long MinimumPartitionSize = 10_000;

    /// <summary>Largest <see cref="PartitionSize"/> allowed.</summary>
    internal const long MaximumPartitionSize = 1_000_000_000_000_000;

    /// <summary>
    /// Test seam. Allows a <see cref="PartitionSize"/> below <see cref="MinimumPartitionSize"/>,
    /// so tests cross many ranges with few events.
    /// </summary>
    internal bool AllowTinyPartitionsForTests { get; set; }

    /// <summary>True when this store uses position ranges, after the tenancy default is applied.</summary>
    internal bool UsesPositionPartitions => PositionPartitioning ?? Tenancy != Tenancy.Isolated;

    /// <summary>
    /// Send a wake-up after a committed append. Default true.
    /// The wake-up carries the newest position only. Set false to skip it.
    /// Subscriptions then poll, and a save in this process still wakes them.
    /// </summary>
    public bool NotifyOnAppend { get; set; } = true;

    /// <summary>
    /// How long an idle subscription waits between reads while the listener is connected.
    /// A wake-up ends the wait early. Default 1 second.
    /// </summary>
    public TimeSpan BackupPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// How long an idle subscription waits between reads when no listener is connected.
    /// A save in this process still ends the wait early. Default 100 milliseconds.
    /// </summary>
    public TimeSpan PollOnlyInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Distinct tags on one append above this count take one store-wide lock
    /// instead of one lock per tag. Default 64. A hash of two different tags
    /// only causes extra waiting.
    /// </summary>
    public int AdvisoryLockTagThreshold { get; set; } = 64;

    /// <summary>
    /// How long one unfinished write may hold the caught-up sequence.
    /// Default 30 seconds. After that, readers move to the highest committed
    /// sequence and the store logs a warning. The warning names the session
    /// and how long the write has been open.
    /// <see cref="Timeout.InfiniteTimeSpan"/> waits until the write ends.
    /// A row that commits below a head returned after this move is logged as an error.
    /// </summary>
    public TimeSpan ReadHorizonGracePeriod { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Retries for a serialization failure, a deadlock, or a connection that never opened. Default 3.</summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>Base delay between those retries. The delay grows with the attempt number. Default 200 ms.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Per-handle channel capacity. When full, delivery waits. Default 10000.</summary>
    public int SubscriptionChannelCapacity { get; set; } = 10_000;

    /// <summary>Maximum events read per subscription page. Default 500.</summary>
    public int SubscriptionBatchSize { get; set; } = 500;

    /// <summary>Checks counts and names before the store is used.</summary>
    public void EnsureValid()
    {
        TenancyValidation.EnsureValid(Tenancy, SingleTenantId, ValidateNames);

        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(ConnectionString));

        PgIdentifier.RequireSchema(SchemaName);

        var minimumSize = AllowTinyPartitionsForTests ? 1 : MinimumPartitionSize;
        if (PartitionSize < minimumSize || PartitionSize > MaximumPartitionSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(PartitionSize),
                PartitionSize,
                "PartitionSize must be from 10,000 to 1,000,000,000,000,000.");
        }

        if (UsesPositionPartitions)
        {
            RequireRangeTableName(EventsTableName, nameof(EventsTableName));
            RequireRangeTableName(EventTagsTableName, nameof(EventTagsTableName));
        }

        if (AdvisoryLockTagThreshold < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(AdvisoryLockTagThreshold),
                AdvisoryLockTagThreshold,
                "AdvisoryLockTagThreshold must be at least 1.");
        }

        if (BackupPollInterval < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(BackupPollInterval),
                BackupPollInterval,
                "BackupPollInterval cannot be negative.");
        }

        if (PollOnlyInterval < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(PollOnlyInterval),
                PollOnlyInterval,
                "PollOnlyInterval cannot be negative.");
        }

        if (string.IsNullOrWhiteSpace(OutboxTableName))
            throw new ArgumentException("OutboxTableName is required.", nameof(OutboxTableName));
    }

    /// <summary>Range tables are named <c>{table}_p{n}</c>. Postgres cuts names longer than 63 bytes.</summary>
    private static void RequireRangeTableName(string table, string name)
    {
        if (string.IsNullOrWhiteSpace(table))
            throw new ArgumentException(name + " is required.", name);
        if (System.Text.Encoding.UTF8.GetByteCount(table) > MaxRangeTableNameBytes)
        {
            throw new ArgumentException(
                $"{name} must be at most {MaxRangeTableNameBytes} bytes when position ranges are on, so each range table name fits.",
                name);
        }
    }

    /// <summary>Longest events or event_tags table name allowed with position ranges.</summary>
    internal const int MaxRangeTableNameBytes = 40;
}
