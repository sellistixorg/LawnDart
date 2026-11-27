using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// Quoted names for one context schema.
/// </summary>
internal sealed class PostgresNames
{
    private readonly string _eventsTable;
    private readonly string _streamsTable;
    private readonly string _tagsTable;
    private readonly string _streamTagsTable;

    public PostgresNames(PostgresEventStoreOptions options)
    {
        Schema = PgIdentifier.RequireSchema(options.SchemaName);
        _eventsTable = options.EventsTableName;
        _streamsTable = options.StreamsTableName;
        _tagsTable = options.EventTagsTableName;
        _streamTagsTable = StreamTagsName(_streamsTable);
        StreamTagsTable = _streamTagsTable;
        Events = PgIdentifier.Qualify(Schema, _eventsTable);
        Streams = PgIdentifier.Qualify(Schema, _streamsTable);
        StreamTags = PgIdentifier.Qualify(Schema, _streamTagsTable);
        Tags = PgIdentifier.Qualify(Schema, _tagsTable);
        Types = PgIdentifier.Qualify(Schema, options.EventTypesTableName);
        SchemaInfo = PgIdentifier.Qualify(Schema, options.SchemaInfoTableName);
        Outbox = PgIdentifier.Qualify(Schema, options.OutboxTableName);
        Sequence = PgIdentifier.Qualify(Schema, "event_position");
        Append = PgIdentifier.Qualify(Schema, "append_to_stream");
        EnsurePartitions = PgIdentifier.Qualify(Schema, EnsurePartitionsFunction);
        ExactText = PgIdentifier.Qualify(Schema, "exact_text");
        NotifyChannel = NotifyChannelFor(Schema);
    }

    /// <summary>
    /// Wake-up channel for a schema. Postgres rejects a name of 64 bytes or more,
    /// so a longer schema is shortened and a stable hash is added. NOTIFY and LISTEN
    /// both use this string.
    /// </summary>
    internal static string NotifyChannelFor(string schema)
    {
        const int maxBytes = 63;
        var prefixed = "ld_" + schema;
        if (Encoding.UTF8.GetByteCount(prefixed) <= maxBytes)
            return prefixed;

        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(schema)))
            .ToLowerInvariant()[..8];
        var suffix = "_" + hash;
        var budget = maxBytes - Encoding.UTF8.GetByteCount("ld_") - Encoding.UTF8.GetByteCount(suffix);
        return "ld_" + Utf8Prefix(schema, budget) + suffix;
    }

    private static string Utf8Prefix(string value, int maxBytes)
    {
        if (maxBytes <= 0 || value.Length == 0)
            return "";

        var bytes = 0;
        var chars = 0;
        foreach (var rune in value.EnumerateRunes())
        {
            if (bytes + rune.Utf8SequenceLength > maxBytes)
                break;
            bytes += rune.Utf8SequenceLength;
            chars += rune.Utf16SequenceLength;
        }

        return value[..chars];
    }

    public string Schema { get; }

    public string Events { get; }

    public string Streams { get; }

    /// <summary>One row per stream tag. The primary key is (tenant_id, tag, stream_id).</summary>
    public string StreamTags { get; }

    /// <summary>Unqualified stream tag table name.</summary>
    internal string StreamTagsTable { get; }

    public string Tags { get; }

    public string Types { get; }

    public string SchemaInfo { get; }

    /// <summary>Quoted outbox table. Not partitioned.</summary>
    public string Outbox { get; }

    public string Sequence { get; }

    public string Append { get; }

    /// <summary>Function that creates position ranges up to a target.</summary>
    public string EnsurePartitions { get; }

    /// <summary>Unqualified name of the range function.</summary>
    internal const string EnsurePartitionsFunction = "ensure_event_partitions";

    /// <summary>Unquoted events table name. Range tables are this name plus <c>_p</c> and the range number.</summary>
    internal string EventsTable => _eventsTable;

    /// <summary>Unquoted tag table name. Range tables are this name plus <c>_p</c> and the range number.</summary>
    internal string TagsTable => _tagsTable;

    /// <summary>Quoted range table of the events table for range <paramref name="range"/>.</summary>
    public string EventsRange(long range) => PgIdentifier.Qualify(Schema, _eventsTable + "_p" + range.ToString(CultureInfo.InvariantCulture));

    /// <summary>Quoted range table of the tag table for range <paramref name="range"/>.</summary>
    public string TagsRange(long range) => PgIdentifier.Qualify(Schema, _tagsTable + "_p" + range.ToString(CultureInfo.InvariantCulture));

    public string ExactText { get; }

    /// <summary>NOTIFY channel. The payload is the newest position, never an event.</summary>
    public string NotifyChannel { get; }

    private static string StreamTagsName(string streamsTable)
        => string.Equals(streamsTable, "streams", StringComparison.Ordinal)
            ? "stream_tags"
            : streamsTable + "_tags";
}
