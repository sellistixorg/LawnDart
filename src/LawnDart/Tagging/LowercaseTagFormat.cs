using System.Globalization;

namespace LawnDart.Tagging;

/// <summary>
/// Writes a lowercase type, a colon, and the id.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="Guid"/> is written with format <c>D</c> (hyphens).
/// A string id is kept as given, so a tenant code is not rewritten.
/// </para>
/// <para>
/// <see cref="LawnDartExtensions.AddLawnDart"/> registers this type as the
/// unkeyed <see cref="ITagFormat"/> unless one is already registered.
/// </para>
/// </remarks>
public sealed class LowercaseTagFormat : ITagFormat
{
    /// <summary>
    /// Shared instance.
    /// </summary>
    public static LowercaseTagFormat Instance { get; } = new();

    /// <inheritdoc />
    public string Tag(string type, Guid id) => Tag(type, id.ToString("D"));

    /// <inheritdoc />
    public string Tag(string type, string id)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(type);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        return string.Create(CultureInfo.InvariantCulture, $"{type.ToLowerInvariant()}:{id}");
    }
}
