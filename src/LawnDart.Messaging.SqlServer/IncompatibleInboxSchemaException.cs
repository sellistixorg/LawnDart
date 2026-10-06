namespace LawnDart.Messaging.SqlServer;

/// <summary>
/// The inbox table exists but is not this process's schema format.
/// Drop and recreate the table. Pre-1.0 has no in-place migration.
/// </summary>
public sealed class IncompatibleInboxSchemaException : InvalidOperationException
{
    /// <summary>Schema-qualified inbox table that failed the format check.</summary>
    public string TableName { get; }

    /// <summary>
    /// Stamp read from <c>LawnDart_InboxSchemaFormat</c>, or <see langword="null"/>
    /// when the table has no stamp.
    /// </summary>
    public string? FoundFormat { get; }

    /// <summary>Format this process writes on a fresh create.</summary>
    public string RequiredFormat { get; }

    /// <summary>Creates the exception. The message names drop-and-recreate.</summary>
    public IncompatibleInboxSchemaException(string tableName, string? foundFormat, string requiredFormat)
        : this(tableName, foundFormat, requiredFormat, shapeMismatch: false)
    {
    }

    internal IncompatibleInboxSchemaException(
        string tableName,
        string? foundFormat,
        string requiredFormat,
        bool shapeMismatch)
        : base(BuildMessage(tableName, foundFormat, requiredFormat, shapeMismatch))
    {
        TableName = tableName;
        FoundFormat = foundFormat;
        RequiredFormat = requiredFormat;
    }

    private static string BuildMessage(
        string tableName,
        string? foundFormat,
        string requiredFormat,
        bool shapeMismatch)
    {
        var found = foundFormat is null
            ? "no LawnDart_InboxSchemaFormat stamp"
            : $"'{foundFormat}'";
        var shape = shapeMismatch
            ? " The column shape does not match MessageId NVARCHAR(256) NOT NULL primary key and ProcessedAt DATETIMEOFFSET(7) NOT NULL."
            : string.Empty;
        return
            $"Inbox table '{tableName}' is not LawnDart inbox schema format {requiredFormat} " +
            $"(found {found}).{shape} " +
            "Drop and recreate the table. Pre-1.0 schema changes are a wipe; there is no in-place ALTER.";
    }
}
