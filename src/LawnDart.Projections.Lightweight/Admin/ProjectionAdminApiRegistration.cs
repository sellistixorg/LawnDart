namespace LawnDart.Projections.Lightweight.Admin;

/// <summary>
/// Tracks which bounded contexts had rebuild endpoints mapped by
/// <c>MapProjectionAdminApi</c>. Registered as a singleton by
/// <c>WithProjections</c> so the debugger UX can gate Rebuild controls.
/// </summary>
public sealed class ProjectionAdminApiRegistration
{
    private readonly HashSet<string> _mappedContextNames =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets the options last applied by <c>MapProjectionAdminApi</c>.
    /// </summary>
    public ProjectionAdminApiOptions Options { get; internal set; } = new();

    /// <summary>
    /// Gets the context names for which rebuild routes were mapped.
    /// </summary>
    public IReadOnlyCollection<string> MappedContextNames => _mappedContextNames;

    /// <summary>
    /// Returns <see langword="true"/> when rebuild was mapped for
    /// <paramref name="contextName"/>.
    /// </summary>
    public bool IsRebuildMapped(string contextName)
        => !string.IsNullOrWhiteSpace(contextName) && _mappedContextNames.Contains(contextName);

    internal void MarkMapped(string contextName) => _mappedContextNames.Add(contextName);
}
