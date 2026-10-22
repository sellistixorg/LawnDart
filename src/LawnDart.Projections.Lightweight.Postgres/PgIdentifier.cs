namespace LawnDart.Projections.Lightweight.Postgres;

/// <summary>
/// Quotes Postgres identifiers built from configuration.
/// </summary>
internal static class PgIdentifier
{
    /// <summary>Returns a double-quoted identifier. A quote in the name is doubled.</summary>
    internal static string Quote(string name)
        => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    /// <summary>Returns <c>"schema"."name"</c>.</summary>
    internal static string Qualify(string schema, string name)
        => Quote(schema) + "." + Quote(name);

    /// <summary>Rejects a blank schema and the <c>public</c> schema.</summary>
    internal static string RequireSchema(string? schema)
    {
        if (string.IsNullOrWhiteSpace(schema))
            throw new ArgumentException("Schema name is required. The default context uses lawndart.", nameof(schema));

        if (string.Equals(schema, "public", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Schema name 'public' is the public schema. Pick another schema. The default context uses lawndart.",
                nameof(schema));
        }

        return schema;
    }
}
