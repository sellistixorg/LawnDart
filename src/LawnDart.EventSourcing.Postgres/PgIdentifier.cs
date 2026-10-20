namespace LawnDart.EventSourcing.Postgres;

/// <summary>
/// Quotes Postgres identifiers built from configuration.
/// </summary>
internal static class PgIdentifier
{
    /// <summary>
    /// Returns a double-quoted identifier. A quote in <paramref name="name"/> is doubled.
    /// </summary>
    internal static string Quote(string name)
        => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    /// <summary>
    /// Returns <c>"schema"."name"</c>.
    /// </summary>
    internal static string Qualify(string schema, string name)
        => Quote(schema) + "." + Quote(name);

    /// <summary>
    /// Rejects a blank schema and the <c>public</c> schema.
    /// </summary>
    internal static string RequireSchema(string? schema)
    {
        if (string.IsNullOrWhiteSpace(schema))
            throw new ArgumentException("SchemaName is required. The default is lawndart.", nameof(schema));

        if (string.Equals(schema, "public", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "SchemaName 'public' is the public schema. Pick another schema. The default is lawndart.",
                nameof(schema));
        }

        return schema;
    }
}
