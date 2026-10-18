namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Prefix match for stream ids. <c>_</c>, <c>%</c>, and <c>[</c> are literal.
/// </summary>
/// <remarks>
/// The predicate does not apply a <c>COLLATE</c> clause. SQL Server then uses the
/// <c>StreamId</c> column collation, so <c>LIKE 'prefix%'</c> can seek the primary key.
/// A new database is case-insensitive. The in-memory store uses
/// <see cref="StringComparison.OrdinalIgnoreCase"/>.
/// </remarks>
internal static class StreamIdPrefixSql
{
    /// <summary>
    /// Escapes <c>\</c>, <c>%</c>, <c>_</c>, and <c>[</c>, then appends the trailing wildcard.
    /// </summary>
    internal static string LikeParameter(string prefix)
    {
        var escaped = prefix
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("%", "\\%", StringComparison.Ordinal)
            .Replace("_", "\\_", StringComparison.Ordinal)
            .Replace("[", "\\[", StringComparison.Ordinal);
        return escaped + "%";
    }

    /// <summary>
    /// SQL predicate. <paramref name="column"/> is a trusted identifier, not user input.
    /// No <c>COLLATE</c> clause, so the column index stays usable.
    /// </summary>
    internal static string Predicate(string column) =>
        $"{column} LIKE @Prefix ESCAPE '\\'";
}
