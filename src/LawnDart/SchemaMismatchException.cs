namespace LawnDart;

/// <summary>
/// The database schema does not match the version that read it.
/// </summary>
public sealed class SchemaMismatchException : InvalidOperationException
{
    /// <summary>Creates an exception for a check that did not match.</summary>
    /// <param name="result">The check that did not match.</param>
    public SchemaMismatchException(SchemaCheckResult result)
        : base(Describe(result))
    {
        ArgumentNullException.ThrowIfNull(result);
        Result = result;
    }

    /// <summary>The check that did not match.</summary>
    public SchemaCheckResult Result { get; }

    private static string Describe(SchemaCheckResult result)
    {
        var stamp = result.ExpectedFormat is null
            ? "This store does not keep a format stamp."
            : $"Expected format stamp {result.ExpectedFormat}. Found {(result.FoundFormat is null ? "no format stamp" : result.FoundFormat)}.";
        if (result.Missing.Count == 0)
            return "The database schema does not match this version. " + stamp;

        return "The database schema does not match this version. " + stamp
            + " Missing: " + string.Join(", ", result.Missing) + ".";
    }
}
