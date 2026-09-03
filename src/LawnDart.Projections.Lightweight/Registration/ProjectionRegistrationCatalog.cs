namespace LawnDart.Projections.Lightweight.Registration;

/// <summary>
/// Resolves lightweight projection registrations by logical name and version.
/// </summary>
public sealed class ProjectionRegistrationCatalog
{
    private readonly IReadOnlyList<ProjectionRegistration> _registrations;
    private readonly Dictionary<string, ProjectionLogicalFamily> _families;

    /// <summary>
    /// Initializes a new <see cref="ProjectionRegistrationCatalog"/>.
    /// </summary>
    public ProjectionRegistrationCatalog(IReadOnlyList<ProjectionRegistration> registrations)
    {
        _registrations = registrations ?? throw new ArgumentNullException(nameof(registrations));
        _families = registrations
            .GroupBy(r => r.LogicalName, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                g => g.Key,
                g =>
                {
                    var versions = g.OrderBy(r => r.Version).ToList();
                    var latest = versions.FirstOrDefault(r => r.IsLatest)
                        ?? versions.OrderByDescending(r => r.Version).First();
                    var latestResolutionMode = versions.Any(r => r.IsLatest)
                        ? latest.LatestResolutionMode
                        : versions.Count == 1
                            ? LatestVersionResolutionMode.SingleVersionImplicit
                            : LatestVersionResolutionMode.HighestVersionFallback;
                    return new ProjectionLogicalFamily
                    {
                        LogicalName = g.Key,
                        KebabName = ToKebabCase(g.Key),
                        Versions = versions,
                        Latest = latest,
                        LatestResolutionMode = latestResolutionMode
                    };
                },
                StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Gets all discovered registrations.
    /// </summary>
    public IReadOnlyList<ProjectionRegistration> Registrations => _registrations;

    /// <summary>
    /// Gets all logical projection families.
    /// </summary>
    public IReadOnlyList<ProjectionLogicalFamily> Families => _families.Values.OrderBy(f => f.LogicalName, StringComparer.OrdinalIgnoreCase).ToList();

    /// <summary>
    /// Resolves a registration by logical name and optional version.
    /// When <paramref name="version"/> is <see langword="null"/>, the family's latest/default version is returned.
    /// </summary>
    public ProjectionRegistration Resolve(string logicalName, int? version = null)
    {
        if (!_families.TryGetValue(logicalName, out var family))
            throw new InvalidOperationException(
                $"Projection '{logicalName}' is not registered. " +
                $"Available: {string.Join(", ", Families.Select(f => f.LogicalName))}");

        if (version is null)
            return family.Latest;

        return family.Versions.FirstOrDefault(r => r.Version == version.Value)
            ?? throw new InvalidOperationException(
                $"Projection '{logicalName}' does not have version v{version.Value}. " +
                $"Available versions: {string.Join(", ", family.Versions.Select(v => $"v{v.Version}"))}");
    }

    /// <summary>
    /// Attempts to get a logical projection family.
    /// </summary>
    public bool TryGetFamily(string logicalName, out ProjectionLogicalFamily family) =>
        _families.TryGetValue(logicalName, out family!);

    /// <summary>
    /// Converts a PascalCase/camelCase projection name into kebab-case for route metadata.
    /// </summary>
    public static string ToKebabCase(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var chars = new List<char>(value.Length + 4);
        for (int i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (char.IsUpper(c))
            {
                if (i > 0 && chars.Count > 0 && chars[^1] != '-')
                    chars.Add('-');
                chars.Add(char.ToLowerInvariant(c));
            }
            else if (c == '_' || c == ' ')
            {
                if (chars.Count > 0 && chars[^1] != '-')
                    chars.Add('-');
            }
            else
            {
                chars.Add(char.ToLowerInvariant(c));
            }
        }

        return new string(chars.ToArray());
    }
}
