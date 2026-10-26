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
        EventsShared = PgIdentifier.Qualify(Schema, _eventsTable + "_shared");
        Streams = PgIdentifier.Qualify(Schema, _streamsTable);
        StreamsShared = PgIdentifier.Qualify(Schema, _streamsTable + "_shared");
        StreamTags = PgIdentifier.Qualify(Schema, _streamTagsTable);
        StreamTagsShared = PgIdentifier.Qualify(Schema, _streamTagsTable + "_shared");
        Tags = PgIdentifier.Qualify(Schema, _tagsTable);
        TagsShared = PgIdentifier.Qualify(Schema, _tagsTable + "_shared");
        Types = PgIdentifier.Qualify(Schema, options.EventTypesTableName);
        SchemaInfo = PgIdentifier.Qualify(Schema, options.SchemaInfoTableName);
        Outbox = PgIdentifier.Qualify(Schema, options.OutboxTableName);
        Sequence = PgIdentifier.Qualify(Schema, "event_position");
        Append = PgIdentifier.Qualify(Schema, "append_to_stream");
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

    public string EventsShared { get; }

    public string Streams { get; }

    public string StreamsShared { get; }

    /// <summary>One row per stream tag. The primary key is (tenant_id, tag, stream_id).</summary>
    public string StreamTags { get; }

    /// <summary>Unqualified stream tag table name.</summary>
    internal string StreamTagsTable { get; }

    public string StreamTagsShared { get; }

    public string Tags { get; }

    public string TagsShared { get; }

    public string Types { get; }

    public string SchemaInfo { get; }

    /// <summary>Quoted outbox table. Not partitioned.</summary>
    public string Outbox { get; }

    public string Sequence { get; }

    public string Append { get; }

    public string ExactText { get; }

    /// <summary>NOTIFY channel. The payload is the newest position, never an event.</summary>
    public string NotifyChannel { get; }

    public string EventsHash(int remainder) => Hash(_eventsTable, remainder);

    public string StreamsHash(int remainder) => Hash(_streamsTable, remainder);

    public string StreamTagsHash(int remainder) => Hash(_streamTagsTable, remainder);

    public string TagsHash(int remainder) => Hash(_tagsTable, remainder);

    private static string StreamTagsName(string streamsTable)
        => string.Equals(streamsTable, "streams", StringComparison.Ordinal)
            ? "stream_tags"
            : streamsTable + "_tags";

    private string Hash(string table, int remainder)
        => PgIdentifier.Qualify(Schema, table + "_shared_h" + remainder.ToString(CultureInfo.InvariantCulture));
}
