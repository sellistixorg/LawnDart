namespace LawnDart.EventSourcing.SqlServer.EventStore;

/// <summary>
/// Prefix match for stream ids. Case-insensitive, and <c>_</c>, <c>%</c>, and <c>[</c> are literal.
/// </summary>
/// <remarks>
/// The in-memory store uses <see cref="StringComparison.OrdinalIgnoreCase"/>.
/// SQL Server uses <see cref="Collation"/> so the database collation does not change the match.
/// For letters A through Z the two agree.
/// </remarks>
internal static class StreamIdPrefixSql
{
    /// <summary>Case-insensitive collation applied to the stream id column in prefix queries.</summary>
    internal const string Collation = "Latin1_General_100_CI_AS";

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

    /// <summary>SQL predicate. <paramref name="column"/> is a trusted identifier, not user input.</summary>
    internal static string Predicate(string column) =>
        $"{column} COLLATE {Collation} LIKE @Prefix ESCAPE '\\'";
}
