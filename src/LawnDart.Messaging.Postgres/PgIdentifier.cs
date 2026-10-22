namespace LawnDart.Messaging.Postgres;

/// <summary>
/// Quotes Postgres identifiers built from configuration.
/// </summary>
internal static class PgIdentifier
{
    internal static string Quote(string name)
        => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    internal static string Qualify(string schema, string name)
        => Quote(schema) + "." + Quote(name);

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
