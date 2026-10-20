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
    /// Optional direct connection for a later listener. Phase 1 does not open it.
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

    /// <summary>
    /// Keep the stream registry up to date on append. Default true.
    /// </summary>
    public bool EnableStreamRegistry { get; set; } = true;

    /// <summary>
    /// When true, a missing tenant id is an error.
    /// When false, a stream with no tenant prefix is stored under <c>default</c>.
    /// Default true.
    /// </summary>
    public bool RequireTenantId { get; set; } = true;

    /// <summary>
    /// When true, a tenant id of <c>default</c> in any casing is rejected, and the id
    /// must match <see cref="LawnDartNames.Pattern"/>.
    /// <c>UsePostgres</c> copies <see cref="LawnDartOptions.ValidateNames"/> onto this
    /// property before the configure callback runs. The callback can change it.
    /// An empty tenant id is still rejected. A colon in a tenant id is still rejected.
    /// </summary>
    public bool ValidateNames { get; set; } = true;

    /// <summary>
    /// The transactional outbox is not in this release.
    /// <c>true</c> is rejected when the store is built.
    /// </summary>
    public bool EnableOutbox { get; set; }

    /// <summary>
    /// How many hash buckets the shared tenant partition uses.
    /// Default 8. Allowed range is 1 to 64. Chosen when the schema is created.
    /// A later open with a different count is rejected.
    /// </summary>
    public int PartitionCount { get; set; } = 8;

    /// <summary>
    /// When true, events, streams, and tags use the two-level tenant partition layout.
    /// Default true. Turning it off after the schema exists is rejected.
    /// </summary>
    public bool Partitioning { get; set; } = true;

    /// <summary>
    /// Send a wake-up notification after a committed append. Default true.
    /// The notification carries the newest position only. Delivery of that
    /// signal to subscribers is a later release. Polling still sees the rows.
    /// </summary>
    public bool NotifyOnAppend { get; set; } = true;

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

    /// <summary>
    /// How long an idle subscription waits before it reads again.
    /// A save that commits in this process also wakes it. Default 75 ms.
    /// </summary>
    public TimeSpan SubscriptionPollInterval { get; set; } = TimeSpan.FromMilliseconds(75);

    /// <summary>Maximum events read per subscription page. Default 500.</summary>
    public int SubscriptionBatchSize { get; set; } = 500;

    /// <summary>Checks counts and names before the store is used.</summary>
    public void EnsureValid()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(ConnectionString));

        PgIdentifier.RequireSchema(SchemaName);

        if (PartitionCount < 1 || PartitionCount > 64)
        {
            throw new ArgumentOutOfRangeException(
                nameof(PartitionCount),
                PartitionCount,
                "PartitionCount must be from 1 to 64.");
        }

        if (AdvisoryLockTagThreshold < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(AdvisoryLockTagThreshold),
                AdvisoryLockTagThreshold,
                "AdvisoryLockTagThreshold must be at least 1.");
        }

        if (EnableOutbox)
        {
            throw new InvalidOperationException(
                "EnableOutbox is not available on the Postgres event store yet. " +
                "Leave EnableOutbox false. The outbox arrives in a later release.");
        }
    }
}
