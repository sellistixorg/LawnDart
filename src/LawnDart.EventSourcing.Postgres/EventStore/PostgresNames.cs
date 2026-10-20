using System.Globalization;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// Quoted names for one context schema.
/// </summary>
internal sealed class PostgresNames
{
    private readonly string _eventsTable;
    private readonly string _streamsTable;
    private readonly string _tagsTable;

    public PostgresNames(PostgresEventStoreOptions options)
    {
        Schema = PgIdentifier.RequireSchema(options.SchemaName);
        _eventsTable = options.EventsTableName;
        _streamsTable = options.StreamsTableName;
        _tagsTable = options.EventTagsTableName;
        Events = PgIdentifier.Qualify(Schema, _eventsTable);
        EventsShared = PgIdentifier.Qualify(Schema, _eventsTable + "_shared");
        Streams = PgIdentifier.Qualify(Schema, _streamsTable);
        StreamsShared = PgIdentifier.Qualify(Schema, _streamsTable + "_shared");
        Tags = PgIdentifier.Qualify(Schema, _tagsTable);
        TagsShared = PgIdentifier.Qualify(Schema, _tagsTable + "_shared");
        Types = PgIdentifier.Qualify(Schema, options.EventTypesTableName);
        SchemaInfo = PgIdentifier.Qualify(Schema, options.SchemaInfoTableName);
        Sequence = PgIdentifier.Qualify(Schema, "event_position");
        Append = PgIdentifier.Qualify(Schema, "append_to_stream");
        ExactText = PgIdentifier.Qualify(Schema, "exact_text");
        NotifyChannel = "ld_" + Schema;
    }

    public string Schema { get; }

    public string Events { get; }

    public string EventsShared { get; }

    public string Streams { get; }

    public string StreamsShared { get; }

    public string Tags { get; }

    public string TagsShared { get; }

    public string Types { get; }

    public string SchemaInfo { get; }

    public string Sequence { get; }

    public string Append { get; }

    public string ExactText { get; }

    /// <summary>NOTIFY channel. The payload is the newest position, never an event.</summary>
    public string NotifyChannel { get; }

    public string EventsHash(int remainder) => Hash(_eventsTable, remainder);

    public string StreamsHash(int remainder) => Hash(_streamsTable, remainder);

    public string TagsHash(int remainder) => Hash(_tagsTable, remainder);

    private string Hash(string table, int remainder)
        => PgIdentifier.Qualify(Schema, table + "_shared_h" + remainder.ToString(CultureInfo.InvariantCulture));
}
