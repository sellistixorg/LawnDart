namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// The schema exists but is not a format this process can open.
/// Drop and recreate the schema. There is no in-place upgrade.
/// </summary>
public sealed class IncompatibleEventStoreSchemaException : InvalidOperationException
{
    /// <summary>Schema that failed the format check.</summary>
    public string SchemaName { get; }

    /// <summary>Format number stored in <c>schema_info</c>, or null when the row is missing.</summary>
    public int? FoundFormat { get; }

    /// <summary>Format this process writes on a fresh create.</summary>
    public int RequiredFormat { get; }

    /// <summary>Creates the exception. The message names drop and recreate.</summary>
    public IncompatibleEventStoreSchemaException(string schemaName, int? foundFormat, int requiredFormat)
        : base(
            $"Postgres event store schema '{schemaName}' is not format {requiredFormat} " +
            $"({(foundFormat is null ? "it has no schema_info row" : $"found format {foundFormat}")}). " +
            "Drop and recreate the schema.")
    {
        SchemaName = schemaName;
        FoundFormat = foundFormat;
        RequiredFormat = requiredFormat;
    }
}
