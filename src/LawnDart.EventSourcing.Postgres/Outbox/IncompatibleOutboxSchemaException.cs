namespace LawnDart.EventSourcing.Postgres.Outbox;

/// <summary>
/// The outbox table exists but is not this process's schema format.
/// Drop and recreate the table.
/// </summary>
public sealed class IncompatibleOutboxSchemaException : InvalidOperationException
{
    /// <summary>Schema-qualified outbox table that failed the format check.</summary>
    public string TableName { get; }

    /// <summary>
    /// Stamp read from <c>schema_info</c> for component <c>outbox</c>,
    /// or <see langword="null"/> when that row is missing.
    /// </summary>
    public int? FoundFormat { get; }

    /// <summary>Format this process writes on a fresh create.</summary>
    public int RequiredFormat { get; }

    /// <summary>Creates the exception. The message names drop and recreate.</summary>
    public IncompatibleOutboxSchemaException(string tableName, int? foundFormat, int requiredFormat)
        : base(BuildMessage(tableName, foundFormat, requiredFormat))
    {
        TableName = tableName;
        FoundFormat = foundFormat;
        RequiredFormat = requiredFormat;
    }

    private static string BuildMessage(string tableName, int? foundFormat, int requiredFormat)
    {
        var found = foundFormat is null
            ? "no outbox row in schema_info"
            : $"'{foundFormat}'";
        return
            $"Outbox table '{tableName}' is not LawnDart outbox schema format {requiredFormat} " +
            $"(found {found}). Drop and recreate the table.";
    }
}
