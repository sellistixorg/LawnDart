namespace LawnDart;

/// <summary>
/// Outcome of a read-only check that the database schema matches this version.
/// </summary>
public sealed class SchemaCheckResult
{
    /// <summary>
    /// Creates a result.
    /// </summary>
    /// <param name="matches">True when the format stamp matches and <paramref name="missing"/> is empty.</param>
    /// <param name="expectedFormat">Format stamp this version writes. Null when this store has no stamp.</param>
    /// <param name="foundFormat">Format stamp read from the database. Null when no stamp was found.</param>
    /// <param name="missing">
    /// Tables, functions, and triggers that are absent.
    /// A function that exists but does not contain this version's marker is listed as
    /// the function name followed by <c> body</c>.
    /// </param>
    public SchemaCheckResult(
        bool matches,
        string? expectedFormat,
        string? foundFormat,
        IReadOnlyList<string> missing)
    {
        ArgumentNullException.ThrowIfNull(missing);
        Matches = matches;
        ExpectedFormat = expectedFormat;
        FoundFormat = foundFormat;
        Missing = missing.Count == 0 ? [] : missing.ToArray();
    }

    /// <summary>True when the format stamp matches and every required object is the one this version writes.</summary>
    public bool Matches { get; }

    /// <summary>Format stamp this version writes. Null when this store has no stamp.</summary>
    public string? ExpectedFormat { get; }

    /// <summary>Format stamp read from the database. Null when no stamp was found.</summary>
    public string? FoundFormat { get; }

    /// <summary>
    /// Tables, functions, and triggers that are absent.
    /// A function that exists but does not contain this version's marker is listed as
    /// the function name followed by <c> body</c>.
    /// Empty when <see cref="Matches"/> is true.
    /// </summary>
    public IReadOnlyList<string> Missing { get; }

    /// <summary>
    /// Returns a match with no missing objects.
    /// <paramref name="expectedFormat"/> is also recorded as the stamp that was found.
    /// </summary>
    /// <param name="expectedFormat">Format stamp this version writes. Null when this store has no stamp.</param>
    /// <returns>A result whose <see cref="Matches"/> is true.</returns>
    public static SchemaCheckResult Match(string? expectedFormat = null)
        => new(true, expectedFormat, expectedFormat, []);

    /// <summary>
    /// Builds a result from a stamp read and the objects that were absent or not this version's body.
    /// The schema matches when the stamp matches and <paramref name="missing"/> is empty.
    /// A store with no stamp passes null for both format values.
    /// </summary>
    /// <param name="expectedFormat">Format stamp this version writes. Null when this store has no stamp.</param>
    /// <param name="foundFormat">Format stamp read from the database. Null when no stamp was found.</param>
    /// <param name="missing">Objects that are absent, or functions whose body is not this version's.</param>
    /// <returns>The check outcome.</returns>
    internal static SchemaCheckResult FromRead(string? expectedFormat, string? foundFormat, IReadOnlyList<string> missing)
    {
        ArgumentNullException.ThrowIfNull(missing);
        var stampOk = expectedFormat is null
            ? foundFormat is null
            : string.Equals(expectedFormat, foundFormat, StringComparison.Ordinal);
        return new SchemaCheckResult(stampOk && missing.Count == 0, expectedFormat, foundFormat, missing);
    }

    /// <summary>
    /// Throws <see cref="SchemaMismatchException"/> when <see cref="Matches"/> is false.
    /// </summary>
    public void ThrowIfMismatch()
    {
        if (!Matches)
            throw new SchemaMismatchException(this);
    }
}
