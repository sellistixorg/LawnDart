namespace LawnDart.Tagging;

/// <summary>
/// Attribute for tagging events with metadata tags for DCB querying.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Property, AllowMultiple = true)]
public class TagAttribute : Attribute
{
    /// <summary>
    /// The tag value.
    /// </summary>
    public string Value { get; }

    /// <summary>
    /// Creates a tag attribute.
    /// </summary>
    /// <param name="value">The tag value (e.g., "order:12345", "high-priority").</param>
    public TagAttribute(string value)
    {
        Value = value ?? throw new ArgumentNullException(nameof(value));
    }
}


