using LawnDart.EventSourcing;
using LawnDart.EventStore;

namespace LawnDart.EventSourcing.SqlServer;

/// <summary>
/// Configuration options for the SQL Server event store.
/// </summary>
public class SqlServerEventStoreOptions : EventSourcingOptions
{
    /// <summary>
    /// The bounded context name used as the DI key when registering multiple SQL Server
    /// event stores in a single application (one per bounded context).
    /// Defaults to <c>"default"</c>.
    /// </summary>
    /// <remarks>
    /// This is the logical identifier consumed by <c>BoundedContextBuilder</c> and
    /// <see cref="Microsoft.Extensions.DependencyInjection.IKeyedServiceProvider"/>.
    /// It does not need to match <see cref="SchemaName"/>; for example, you can use
    /// <c>ContextName = "ordering"</c> with <c>SchemaName = "ordering"</c> for a clean
    /// one-to-one mapping, or keep <c>SchemaName = "dbo"</c> for backward-compatible
    /// migration of an existing single-context schema.
    /// </remarks>
    public string ContextName { get; set; } = "default";

    /// <summary>
    /// The SQL Server schema that contains all tables and sequences for this context.
    /// Defaults to <c>"dbo"</c> for backward compatibility.
    /// </summary>
    /// <remarks>
    /// Each bounded context should use a distinct schema (e.g., <c>"ordering"</c>,
    /// <c>"catalog"</c>) to achieve physical SQL-level isolation, independent SEQUENCE
    /// objects, and clean DDL separation — even within the same database.
    /// The schema is created automatically during <c>InitializeSchemaAsync</c> if it
    /// does not yet exist.
    /// </remarks>
    public string SchemaName { get; set; } = "dbo";

    /// <summary>
    /// SQL Server connection string.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Event store table name.
    /// Default: "Events"
    /// </summary>
    public string EventStoreTableName { get; set; } = "Events";

    /// <summary>
    /// Normalized event tags table name. Used for indexed tag lookups.
    /// Default: "EventTags"
    /// </summary>
    public string EventTagsTableName { get; set; } = "EventTags";

    /// <summary>
    /// Stream registry table name.
    /// Default: "Streams"
    /// </summary>
    public string StreamRegistryTableName { get; set; } = "Streams";

    /// <summary>
    /// Enable automatic stream registry maintenance.
    /// Default: true
    /// </summary>
    public bool EnableStreamRegistry { get; set; } = true;

    /// <summary>
    /// Automatically update registry during event appends.
    /// Default: true
    /// </summary>
    public bool AutoMaintainRegistry { get; set; } = true;

    /// <summary>
    /// Require tenant ID in all operations.
    /// When true, TenantId must be present in CommandMetadata and stream IDs must include tenant prefix.
    /// When false, tenant is optional and stream IDs can be in any format.
    /// Default: true
    /// </summary>
    public bool RequireTenantId { get; set; } = true;

    /// <summary>
    /// Enable transactional outbox pattern.
    /// When true, events are automatically written to the outbox table in the same transaction as event append.
    /// Default: false
    /// </summary>
    public bool EnableOutbox { get; set; } = false;

    /// <summary>
    /// Use a normalized EventTags side-table for indexed tag lookups instead of OPENJSON on Tags column.
    /// Set to false only if migrating an existing schema that does not yet have the EventTags table.
    /// Default: true
    /// </summary>
    public bool UseEventTagsTable { get; set; } = true;

    /// <summary>
    /// DCB snapshot table name. Primary key is a 64-character SHA-256 hex
    /// (<see cref="LawnDart.Dcb.DcbSnapshotId"/>), not plaintext tags.
    /// Default: "DcbSnapshots"
    /// </summary>
    public string DcbSnapshotsTableName { get; set; } = "DcbSnapshots";

    /// <summary>
    /// Stream (aggregate) snapshot table name.
    /// Default: "EventSnapshots"
    /// </summary>
    public string EventSnapshotsTableName { get; set; } = "EventSnapshots";

    /// <summary>
    /// Maximum number of retry attempts for transient SQL errors (e.g. deadlocks, timeouts).
    /// Default: 3
    /// </summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>
    /// Base delay between retry attempts. Actual delay is multiplied by the attempt number (linear back-off).
    /// Default: 200 ms
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Outbox table name.
    /// Default: "Outbox"
    /// </summary>
    public string OutboxTableName { get; set; } = "Outbox";

    /// <summary>
    /// Per-handle bounded channel capacity for portable <see cref="IEventStoreSubscriptions"/>.
    /// When full, the delivery loop blocks (no silent drop). Default: 10000.
    /// </summary>
    public int SubscriptionChannelCapacity { get; set; } = 10_000;

    /// <summary>
    /// Idle poll interval for live subscription delivery when no new matching events are found.
    /// Poll-backed live is intentional for SQL v1 (not CDC). Default: 75 ms.
    /// </summary>
    public TimeSpan SqlSubscriptionPollInterval { get; set; } = TimeSpan.FromMilliseconds(75);

    /// <summary>
    /// Maximum matching events to deliver per catch-up / poll page before looping again.
    /// Default: 500.
    /// </summary>
    public int SqlSubscriptionBatchSize { get; set; } = 500;
}
