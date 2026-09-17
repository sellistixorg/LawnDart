namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// The events table exists but is not this process's schema format.
/// Drop and recreate the table. Pre-1.0 has no in-place migration.
/// </summary>
public sealed class IncompatibleEventStoreSchemaException : InvalidOperationException
{
    /// <summary>Schema-qualified events table that failed the format check.</summary>
    public string TableName { get; }

    /// <summary>
    /// Stamp read from <c>LawnDart_SchemaFormat</c>, or <see langword="null"/> when the
    /// table has no stamp.
    /// </summary>
    public string? FoundFormat { get; }

    /// <summary>Format this process writes on a fresh create.</summary>
    public string RequiredFormat { get; }

    /// <summary>Creates the exception. The message names drop-and-recreate.</summary>
    public IncompatibleEventStoreSchemaException(string tableName, string? foundFormat, string requiredFormat)
        : base(
            $"Events table '{tableName}' is not LawnDart schema format {requiredFormat} " +
            $"(found {(foundFormat is null ? "no LawnDart_SchemaFormat stamp" : $"'{foundFormat}'")}). " +
            "Drop and recreate the table. Pre-1.0 schema changes are a wipe; there is no in-place ALTER.")
    {
        TableName = tableName;
        FoundFormat = foundFormat;
        RequiredFormat = requiredFormat;
    }
}
