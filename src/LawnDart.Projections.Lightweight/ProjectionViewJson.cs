using System.Text.Json;

namespace LawnDart.Projections.Lightweight;

/// <summary>
/// JSON serializer options for lightweight projection view snapshots: view-store
/// persistence, time-travel / debug APIs, and ad-hoc replay payloads.
/// </summary>
/// <remarks>
/// <para>
/// Writes <see cref="JsonNamingPolicy.CamelCase"/> so stored JSON and HTTP payloads align with
/// typical JavaScript and ASP.NET Core JSON defaults. Reads use
/// <see cref="JsonSerializerOptions.PropertyNameCaseInsensitive"/> so older snapshots written with
/// PascalCase property names still deserialize into CLR view types.
/// </para>
/// <para>
/// Event payloads and other subsystems may use different policies; this type applies only to
/// projection view DTO serialization in the lightweight projection stack.
/// </para>
/// </remarks>
public static class ProjectionViewJson
{
    /// <summary>Options for serializing projection views to JSON strings.</summary>
    public static readonly JsonSerializerOptions Write = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    /// <summary>
    /// Options for deserializing projection views from JSON strings. Pair with
    /// <see cref="Write"/>; also accepts legacy PascalCase keys from older writers.
    /// </summary>
    public static readonly JsonSerializerOptions Read = new()
    {
        PropertyNameCaseInsensitive = true,
    };
}
