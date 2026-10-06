namespace LawnDart.Sql;

/// <summary>
/// Escapes SQL Server identifiers and string literals built from configuration.
/// </summary>
internal static class SqlIdentifier
{
    /// <summary>
    /// Doubles <c>]</c> so the name can be placed inside brackets by the caller.
    /// </summary>
    internal static string Escape(string name)
        => name.Replace("]", "]]", StringComparison.Ordinal);

    /// <summary>
    /// Returns a bracketed identifier. A <c>]</c> in <paramref name="name"/> is doubled.
    /// </summary>
    internal static string Quote(string name)
        => "[" + Escape(name) + "]";

    /// <summary>
    /// Returns <c>[schema].[table]</c> with each part bracketed.
    /// </summary>
    internal static string Qualify(string schema, string table)
        => Quote(schema) + "." + Quote(table);

    /// <summary>
    /// Doubles <c>'</c> for a value placed inside a SQL string literal.
    /// </summary>
    internal static string Literal(string value)
        => value.Replace("'", "''", StringComparison.Ordinal);
}
